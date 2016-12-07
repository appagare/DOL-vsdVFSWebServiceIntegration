Option Strict On
Option Explicit On 

Imports System.ServiceProcess
Imports WA.DOL.LogEvent.LogEvent
Imports WA.DOL.Data
Imports System.Threading

Public Class vsdVFSWebServiceIntegration
    Inherits System.ServiceProcess.ServiceBase

    'db connection paramaters
    Private DBConnectRetryDelay As Integer = 30 'delay 30 sec. between initial DB connect attempts unless specified in app.config
    Private DBConnectRetryMax As Integer = 0 'try to obtain DB parameters indefinitely unless specified in app.config

    Private LogEventObject As New WA.DOL.LogEvent.LogEvent 'common LogEvent object
    Private DataObject As WA.DOL.Data.SqlHelper 'common Data object
    Private ThreadCount As Integer = 0 'number of threads spawned
    Private ConfigValues As New ConfigValues 'common class to hold all of the common runtime parameters
    Private Credentials As Net.ICredentials 'interface for making secure web service calls
    Private Proxy As System.Net.WebProxy 'option proxy object
    Private PerformanceTestMode As Boolean = False
    Private Enum ResponseStatus
        NotSet = -1
        Success = 0
        Recoverable = 1
        NonRecoverable = 2
        RecoverableWithEmail = 3
    End Enum

    'enumeration for state of the service
    Private Enum ServiceStates
        Shutdown = 0
        Paused = 1
        Running = 2
    End Enum
    'enumeration for column positions
    Private Enum Columns
        TRAN_TYPE_IDX = 0
        KEY_IDX = 1
        ATTEMPT_IDX = 2
        FAILED_IDX = 3
        FIRST_BUFFER_IDX = 4
    End Enum

    Private ServiceState As ServiceStates = ServiceStates.Paused

#Region " Component Designer generated code "

    Public Sub New()
        MyBase.New()

        ' This call is required by the Component Designer.
        InitializeComponent()

        ' Add any initialization after the InitializeComponent() call

    End Sub

    'UserService overrides dispose to clean up the component list.
    Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
        If disposing Then
            If Not (components Is Nothing) Then
                components.Dispose()
            End If
        End If
        MyBase.Dispose(disposing)
    End Sub

    ' The main entry point for the process
    <MTAThread()> _
    Shared Sub Main()
        Dim ServicesToRun() As System.ServiceProcess.ServiceBase

        ' More than one NT Service may run within the same process. To add
        ' another service to this process, change the following line to
        ' create a second service object. For example,
        '
        '   ServicesToRun = New System.ServiceProcess.ServiceBase () {New Service1, New MySecondUserService}
        '
        ServicesToRun = New System.ServiceProcess.ServiceBase() {New vsdVFSWebServiceIntegration}

        System.ServiceProcess.ServiceBase.Run(ServicesToRun)
    End Sub

    'Required by the Component Designer
    Private components As System.ComponentModel.IContainer

    ' NOTE: The following procedure is required by the Component Designer
    ' It can be modified using the Component Designer.  
    ' Do not modify it using the code editor.
    <System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
        '
        'vsdVFSWebServiceIntegration
        '
        Me.ServiceName = "vsdVFSWebServiceIntegration"

    End Sub

#End Region

    Protected Overrides Sub OnStart(ByVal args() As String)
        ' Add code here to start your service. This method should set things
        ' in motion so your service can do its work.

        Try
            'read the basic operating parameters
            ReadAppSettings()

        Catch ex As Exception

            'LogEvent, Send E-mail, and quit
            Dim strMessage As String = "Service is unable to proceed. Shutting down. " & ex.Message
            'log the error
            LogEvent("Service_OnStart", strMessage, MessageType.Error, LogType.Standard)

            'initiate stop process
            InitiateStop()
            Exit Sub
        End Try

        'start an endless loop for service processing the queue
        ThreadPool.QueueUserWorkItem(AddressOf ServiceRun)
    End Sub

    Protected Overrides Sub OnStop()
        ' Add code here to perform any tear-down necessary to stop your service.
        'warn threads we are shutting down
        ServiceState = ServiceStates.Shutdown

        'log the fact that we are "starting to stop"
        LogEvent("OnStop", "Begin OnStop", MessageType.Information, LogType.Standard)

        'give threads up to "Delay" to wrap things up (Delay is in milliseconds)
        Dim dtEndWait As Date = Now.AddMilliseconds(ConfigValues.Delay)
        While Now <= dtEndWait
            If ThreadCount = 0 Then
                Exit While
            End If
        End While


        'log event that we have stopped
        LogEvent("OnStop", "Service Stopped", MessageType.Finish, LogType.Standard)

        LogEventObject = Nothing
        ConfigValues = Nothing
    End Sub

    Protected Overrides Sub OnShutdown()
        'calls the Windows service OnStop method with the OS shuts down.
        OnStop()
    End Sub

    ''' <summary>
    '''     Programmatically stop the main thread if we've already started up
    '''     such as during the database connection
    ''' </summary>
    Private Sub InitiateStop()
        Dim sc As New ServiceController(Me.ServiceName)
        sc.Stop()
        sc = Nothing
    End Sub
    ' <summary>
    '''     Returns the status value of the web method, based on the code.
    ''' </summary>
    ''' <param name="ResponseCode">Response code that was parsed from the web method call.</param>
    ''' <remarks>
    '''     The ResponseCode is checked against the ResponseValues datatable. If found, the 
    '''     associated value is returned. If not found, the transaction is determined to be 
    '''     recoverable and will be returned to the queue.
    '''     
    '''     Note - This function uses the Find method on the ResponseValues table. "Find" should be 
    '''     thread-safe when reading and it *should* only require synchronization when performing 
    '''     writes. However, If unpredictable results occur, particularly
    '''     under high volumes, consider adding synchronization by uncommenting 
    '''     the SyncLock and End SyncLock lines (synchronization may affect performance).
    '''     SyncLock ConfigValues.ResponseValues
    ''' </remarks>
    Private Function GetStatus(ByVal ResponseCode As String) As ResponseStatus
        Dim ReturnValue As ResponseStatus = ResponseStatus.NotSet
        Dim r As DataRow
        Dim KeyValues(1) As Object
        KeyValues(0) = ConfigValues.ProcessMode
        KeyValues(1) = ResponseCode

        If ConfigValues.DebugMode > 1 Then
            Me.LogEvent("GetStatus", "Debug ResponseCode: [" & ResponseCode & "]", MessageType.Debug, LogType.Standard)
        End If

        'SyncLock ConfigValues.ResponseValues
        r = ConfigValues.ResponseValues.Rows.Find(KeyValues)
        If Not r Is Nothing Then
            ReturnValue = CType(r("ResponseValue"), ResponseStatus)
        Else
            ReturnValue = ResponseStatus.Recoverable
        End If
        'End SyncLock

        'return the result
        Return ReturnValue

    End Function
    ''' <summary>
    '''     Common code for writing an event to the event log database of the specified type.
    ''' </summary>
    ''' <param name="Source">source procedure reporting the event.</param>
    ''' <param name="Message">actual event message.</param>
    ''' <param name="MessageType">LogEvent object indicator specifying whether the message is error, informational, start, finish, or debug.</param>
    ''' <param name="LogType">LogEvent object indicator specifying the type of event to log (Standard, E-mail, etc.)</param>
    ''' <param name="ForceEmail">Forces an e-mail to be sent, regardless of error type or whether or </param>
    ''' <remarks>
    '''     When a LogType is error, an e-mail may be automatically sent. To avoid flooding the AppSupport Inbox, e-mails 
    '''     are only sent once every ConfigValues.EmailFrequency seconds UNLESS the ForceEmail flag is set.
    ''' </remarks>
    Private Sub LogEvent(ByVal Source As String, _
        ByVal Message As String, _
        ByVal MessageType As MessageType, _
        ByVal LogType As LogType, _
        Optional ByVal ForceEmail As Boolean = False)

        'log message
        LogEventObject.LogEvent(Me.ServiceName, Source, Message, MessageType, LogType)

        'if message type is an error, also log an e-mail event if we haven't sent one in awhile
        If ForceEmail = True Or (MessageType = MessageType.Error AndAlso Now >= ConfigValues.LastEmailSent.AddSeconds(ConfigValues.EmailFrequency)) Then

            'send the e-mail
            LogEventObject.LogEvent(Me.ServiceName, Source, Message, MessageType, LogType.Email)

            'update the last email sent time
            ConfigValues.LastEmailSent = Now
        End If
    End Sub
    ''' <summary>
    '''     Encapsulate the web service call.
    ''' </summary>
    ''' <param name="TranTypeUtil">TranTypeUtility class for making the web service call.</param>
    ''' <param name="bytIdx">Index to determine which URL and web method to call.</param>
    ''' <param name="strRequest">String buffer to pass to the web service.</param>
    ''' <param name="IntegrationFailed">ByRef integer for marking the call as failed if the web method is not recognized.</param>
    ''' <param name="datWebEnd">Date/time to receive the web service end time, for stat purposes</param>
    ''' <remarks>
    '''     When a LogType is error, an e-mail may be automatically sent. To avoid flooding the AppSupport Inbox, e-mails 
    '''     are only sent once every ConfigValues.EmailFrequency seconds UNLESS the ForceEmail flag is set.
    ''' </remarks>
    Private Function MakeWebServiceCall(ByRef TranTypeUtil As TranTypeUtility, _
        ByVal bytIdx As Byte, _
        ByVal strRequest As String, _
        ByRef IntegrationFailed As Integer, _
        ByRef datWebEnd As Date) As String

        Dim strResponse As String = ""

        'note to future coders - if online update supports different URLs, 
        'obtaining the web reference may involve polymorphism or a commmon 
        'interface to handle different web objects
        Dim WS As New dolVFSService.dolVFSservice(TranTypeUtil.URL(bytIdx))

        'if we have credentials, assign them to the web service request
        If Not Credentials Is Nothing Then
            WS.Credentials = Credentials
        End If

        'if we have a proxy, assign it to the web service request
        If Not Proxy Is Nothing Then
            WS.Proxy = Proxy
        End If

        Select Case UCase(TranTypeUtil.WebMethod(bytIdx))
            Case "MCTCAP"
                strResponse = WS.CallMCTCAP(strRequest)
            Case "MFLCAP"
                strResponse = WS.CallMFLCAP(strRequest)
            Case "MMCFWD"
                strResponse = WS.CallMMCFWD(strRequest)
            Case "MMCINQ"
                strResponse = WS.CallMMCINQ(strRequest)
            Case "MMDEST"
                strResponse = WS.CallMMDEST(strRequest)
            Case "MMDOEU"
                strResponse = WS.CallMMDOEU(strRequest)
            Case "MMDROS"
                strResponse = WS.CallMMDROS(strRequest)
            Case "MMVUOU"
                strResponse = WS.CallMMVUOU(strRequest)
            Case "MMZUBO"
                strResponse = WS.CallMMZUBO(strRequest)
            Case "MMZUOU"
                strResponse = WS.CallMMZUOU(strRequest)
            Case "MVBNCI"
                strResponse = WS.CallMVBNCI(strRequest)
            Case Else
                'Test Case #9
                'unknown web method (shouldn't happen unless the buTranType table is "out of whack"
                'mark the record as unrecoverable
                IntegrationFailed = 1
                
        End Select

        'capture the end-time of the web service call
        datWebEnd = Now

        'release the web service resource
        WS = Nothing

        Return strResponse
    End Function


    ''' <summary>
    '''     Worker thread to process the records.
    ''' </summary>
    ''' <param name="State">New thread callback. State contains the TranType</param>
    ''' <remarks>
    '''     This runs in a continuous loop until the service is stopped.
    '''     Multiple threads are spawned for unique connect string key. The thread will sleep when no messages are 
    '''     found or when a recoverable error occurs.
    ''' </remarks>
    Private Sub ProcessRecords(ByVal State As Object)

        Const INTEGRATION_ATTEMPT_PARM As String = "@pintIntegrationAttempt"
        Const INTEGRATION_FAILED_PARM As String = "@pintIntegrationFailed"

        Dim strRequest As String = "" 'web method request
        Dim strResponse As String = "" 'web method response
        Dim bytIdx As Byte = 0 'index for multiple web service calls
        Dim blnSleepThread As Boolean = False

        If ConfigValues.DebugMode > 2 Then
            Me.LogEvent("ProcessRecords", "Starting thread (" & CType(State, String) & ")", MessageType.Debug, LogType.Standard, False)
        End If

        Try
            'create an instance of the TranTypeUtil class for this thread
            Dim TranTypeUtil As New TranTypeUtility(CType(State, String), ConfigValues.TranTypes)

            While ServiceState = ServiceStates.Running
                Dim Records As New DataTable 'table of records needing integration
                Dim r As DataRow

                'extra debugging
                If ConfigValues.DebugMode > 2 Then
                    Me.LogEvent("ProcessRecords", "Loop (" & CType(State, String) & ")", MessageType.Debug, LogType.Standard, False)
                End If

                'get the records
                Records = DataObject.ExecuteDataset(TranTypeUtil.ConnectStringKey, CommandType.StoredProcedure, TranTypeUtil.SPSelectName).Tables(0)

                For Each r In Records.Rows

                    'extra debugging
                    If ConfigValues.DebugMode > 2 Then
                        Me.LogEvent("ProcessRecords", "Record (" & CType(State, String) & ") (" & CType(r(Columns.KEY_IDX), String) & ")", MessageType.Debug, LogType.Standard, False)
                    End If

                    Dim datStart As Date = Now 'used for performance testing (DebugMode > 0)
                    Dim datWebStart As Date = Now 'used for performance testing (DebugMode > 0)
                    Dim datWebEnd As Date = Now 'used for performance testing (DebugMode > 0)
                    Dim IntegrationAttempt As Integer = CType(r(Columns.ATTEMPT_IDX), Integer)
                    Dim IntegrationFailed As Integer = CType(r(Columns.FAILED_IDX), Integer)
                    Dim CallStatus As ResponseStatus = ResponseStatus.NotSet

                    Try

                        'increment the attempt counter
                        IntegrationAttempt += 1

                        If TranTypeUtil.TranTypeIsValid(CType(r(Columns.TRAN_TYPE_IDX), String)) = False Then
                            'unrecognizable TranType
                            IntegrationFailed = 1
                            'logevent and send e-mail
                            Me.LogEvent("ProcessRecords", "Unrecognized TranType (" & CType(r(Columns.TRAN_TYPE_IDX), String) & "). Expected " & CType(State, String), MessageType.Error, LogType.Standard, True)
                        Else
                            'if here, TranType is valid, perform all of the calls
                            For bytIdx = 0 To CByte(TranTypeUtil.CallCount - 1)

                                ' see if this call was successfully made already
                                If TranTypeUtil.IsProcessCallCompleted(bytIdx) = False Then
                                    'call not yet made or was previously unsuccessful - ok to make the call

                                    'build the request string from the row - 
                                    'the first four columns in the row ARE NOT part of the buffer
                                    'col1 = TranType
                                    'col2 = record key
                                    'col3 = IntegrationAttempt
                                    'col4 = IntegrationFailed
                                    Dim c As DataColumn
                                    Dim sb As New System.Text.StringBuilder
                                    Dim ColumnCount As Integer = 0
                                    For Each c In Records.Columns
                                        If ColumnCount >= Columns.FIRST_BUFFER_IDX Then
                                            'everything but the first column is part of the request
                                            sb.Append(CType(r(c.ColumnName), String))
                                        End If
                                        ColumnCount += 1
                                    Next
                                    strRequest = sb.ToString

                                    'extra debugging
                                    If ConfigValues.DebugMode > 1 Then
                                        Me.LogEvent("ProcessMessage", "Debug Request: [" & strRequest & "]", MessageType.Debug, LogType.Standard)
                                    End If

                                    'clear this before each web service call
                                    CallStatus = ResponseStatus.NotSet

                                    datWebStart = Now 'capture the start time of the web service call

                                    'make web service calls
                                    strResponse = MakeWebServiceCall(TranTypeUtil, bytIdx, strRequest, IntegrationFailed, datWebEnd)

                                    'web service was unrecognized;
                                    If IntegrationFailed = 1 Then
                                        'logevent and send e-mail and bail For
                                        Me.LogEvent("MakeWebServiceCall", "Unrecognized TranType (" & CType(r(Columns.TRAN_TYPE_IDX), String) & "). Expected " & CType(State, String), MessageType.Error, LogType.Standard, True)
                                        Exit For
                                    End If

                                    'extra debugging
                                    If ConfigValues.DebugMode > 1 Then
                                        Me.LogEvent("ProcessMessage", "Debug Response: [" & strResponse & "]", MessageType.Debug, LogType.Standard)
                                    End If

                                    'determine the status from the response
                                    CallStatus = GetStatus(Mid(strResponse, TranTypeUtil.ResponseOffset(bytIdx), TranTypeUtil.ResponseLength(bytIdx)))

                                    'respond according to response
                                    Select Case CallStatus
                                        Case ResponseStatus.Success
                                            'message ok
                                            'Online Update may need to update a value here to indicate this process call was successful
                                            IntegrationAttempt = -1
                                            IntegrationFailed = -1
                                        Case ResponseStatus.Recoverable
                                            'if recoverable, check to see if we've given this message a reasonable number of tries to complete
                                            If CType(IntegrationAttempt, Integer) > ConfigValues.MaxProcessAttempts _
                                                AndAlso ConfigValues.MaxProcessAttempts > 0 Then
                                                'we have exhausted our attempts for this record 
                                                'log an error - force e-mail
                                                Me.LogEvent("ProcessRecords", "Maximum attempts for this record have been reached. TranType=" & _
                                                    CType(r(Columns.TRAN_TYPE_IDX), String) & ", Key=" & CType(r(Columns.KEY_IDX), String), MessageType.Error, _
                                                    LogType.Standard, True)
                                                'reset the counter
                                                IntegrationAttempt = 0
                                            Else
                                                'haven't exhausted the tries for this record yet - log an error
                                                Me.LogEvent("ProcessRecords", "Attempt " & IntegrationAttempt.ToString & " for TranType=" & _
                                                    CType(r(Columns.TRAN_TYPE_IDX), String) & ", Key=" & CType(r(Columns.KEY_IDX), String), MessageType.Error, _
                                                    LogType.Standard)
                                            End If
                                            'signal thread to sleep after we update
                                            blnSleepThread = True
                                            Exit For 'exit process call

                                        Case ResponseStatus.RecoverableWithEmail
                                            'if recoverable w/ e-mail, check to see if we've given this message a reasonable number of tries to complete
                                            If CType(IntegrationAttempt, Integer) > ConfigValues.MaxProcessAttempts _
                                                AndAlso ConfigValues.MaxProcessAttempts > 0 Then
                                                'we have exhausted our attempts for this record 
                                                'log an error - force e-mail
                                                Me.LogEvent("ProcessRecords", "NMVTIS brand error. Maximum attempts for this record have been reached. TranType=" & _
                                                    CType(r(Columns.TRAN_TYPE_IDX), String) & ", Key=" & CType(r(Columns.KEY_IDX), String), MessageType.Error, _
                                                    LogType.Standard, True)
                                                'reset the counter
                                                IntegrationAttempt = 0

                                                'signal thread to sleep after we update
                                                blnSleepThread = True
                                            Else
                                                'haven't exhausted the tries for this record yet - log an error and 
                                                'force an e-mail message when this occurs but we don't sleep the thread
                                                'when we haven't reached out attempt tries
                                                Me.LogEvent("ProcessRecords", "NMVTIS brand error. TranType=" & _
                                                    CType(r(Columns.TRAN_TYPE_IDX), String) & ", Key=" & CType(r(Columns.KEY_IDX), String), _
                                                    MessageType.Error, LogType.Standard, True)
                                            End If
                                            Exit For 'exit process call
                                        Case ResponseStatus.NonRecoverable
                                            'if not recoverable, mark as failed. Additional records will continue to be processed
                                            IntegrationFailed = 1
                                            'logevent 
                                            Me.LogEvent("ProcessRecords", "NonRecoverable Response: [" & _
                                                strResponse & "] Request: [" & strRequest & "]", MessageType.Error, LogType.Standard, True)
                                            Exit For 'exit process call
                                        Case Else
                                            'shouldn't happen unless there is coding problem.
                                            'if here it means we recognized the tran type, generated a request
                                            'but failed to set any value based on the response.
                                            Me.LogEvent("ProcessRecords", "Unknown Status: " & CallStatus.ToString & _
                                            " Response: [" & strResponse & "] Request: [" & strRequest & "]", MessageType.Error, LogType.Standard)
                                            Exit For
                                    End Select
                                End If 'ProcessCall already made, skip this
                            Next 'each process call
                        End If

                        'regardless of status, update the record
                        UpdateRecord(TranTypeUtil.ConnectStringKey, TranTypeUtil.SPUpdateName, _
                            TranTypeUtil.SPKeyName, _
                            CType(r(Columns.KEY_IDX), String), _
                            INTEGRATION_ATTEMPT_PARM, IntegrationAttempt, _
                            INTEGRATION_FAILED_PARM, IntegrationFailed)

                        If blnSleepThread = True Then
                            'an error has requested the thread to sleep.
                            'reset the indicator and exit the record set, 
                            'dropping us into Sleep 
                            blnSleepThread = False
                            Exit For 'exit For Next Each record,
                        End If

                    Catch ex As Exception
                        'error processing record - treat as recoverable but sleep all threads
                        If CType(IntegrationAttempt, Integer) > ConfigValues.MaxProcessAttempts _
                                                    AndAlso ConfigValues.MaxProcessAttempts > 0 Then
                            'we have exhausted our attempts for this record 
                            'log an error - force e-mail
                            Me.LogEvent("ProcessRecords", "Error processing record in thread (" & CType(State, String) & "). " & ex.Message & _
                                ". Maximum attempts for this record have been reached. TranType=" & _
                                CType(r(Columns.TRAN_TYPE_IDX), String) & ", Key=" & CType(r(Columns.KEY_IDX), String), MessageType.Error, _
                                LogType.Standard, True)

                            'reset the counter
                            IntegrationAttempt = 0

                        Else
                            'haven't exhausted the tries for this record yet - log an error
                            Me.LogEvent("ProcessRecords", "Error processing record in thread (" & CType(State, String) & "). " & ex.Message & _
                                " TranType=" & CType(r(Columns.TRAN_TYPE_IDX), String) & ", Key=" & CType(r(Columns.KEY_IDX), String), _
                                MessageType.Error, LogType.Standard)
                        End If

                        ''signal threads to sleep - note - we will fall into sleep after we update, either on the next record
                        ''or automatically if no more records exist in the set
                        
                        'update record with attempt count
                        UpdateRecord(TranTypeUtil.ConnectStringKey, TranTypeUtil.SPUpdateName, _
                            TranTypeUtil.SPKeyName, _
                            CType(r(Columns.KEY_IDX), String), _
                            INTEGRATION_ATTEMPT_PARM, IntegrationAttempt, _
                            INTEGRATION_FAILED_PARM, IntegrationFailed)

                        'stop processing this set of records
                        'we will fall into sleep after we update, either on the next record
                        'or automatically if no more records exist in the set
                        Exit For 'exit For Next Each record,
                    End Try 'inner Try to 

                    If ServiceState <> ServiceStates.Running Then
                        'a request to shutdown the service has occured.
                        'bail out of the thread immediately
                        Records = Nothing
                        Exit While
                    End If
                Next 'each record

                'no (more) records or an error forced us here - sleep this thread
                Records = Nothing
                Thread.Sleep(ConfigValues.Delay)

            End While 'main loop - ServiceStates.Running

            TranTypeUtil = Nothing
        Catch ex As Exception
            'critical error - this aborts the thread
            Me.LogEvent("ProcessRecords", "Critical error starting thread (" & CType(State, String) & "). Transactions won't be integrated! " & _
            ex.Message, MessageType.Error, LogType.Standard, True)
        End Try

        'decrement the thread count
        Interlocked.Decrement(ThreadCount)

    End Sub
    ''' <summary>
    '''     Retrieve a single parameter from app.config.
    ''' </summary>
    ''' <param name="Key">The name of the key being retrieved.</param>
    Private Function ReadAppSetting(ByVal Key As String) As String

        On Error Resume Next
        Dim AppSettingsReader As New System.Configuration.AppSettingsReader
        Dim strReturnValue As String = ""
        Key = Trim(Key)
        If Key <> "" Then
            'get the value
            strReturnValue = CType(AppSettingsReader.GetValue(Key, GetType(System.String)), String)
        End If
        AppSettingsReader = Nothing
        Return strReturnValue
    End Function
    ''' <summary>
    '''     Reads the basic app.config values.
    ''' </summary>
    Private Sub ReadAppSettings()
        'Purpose:   Read the basic app.config settings

        'set mode equal to Service Name
        ConfigValues.ProcessMode = Me.ServiceName

        'get DB connect string key
        ConfigValues.ConnectionKey = ReadAppSetting("DatabaseKey") 'get connect string key

        'get DB connect delay
        If IsNumeric(ReadAppSetting("CriticalConnectionRetry")) AndAlso _
            CType(ReadAppSetting("CriticalConnectionRetry"), Integer) > 0 Then
            DBConnectRetryDelay = CType(ReadAppSetting("CriticalConnectionRetry"), Integer)
        End If

        'get DB connect max
        If IsNumeric(ReadAppSetting("CriticalConnectionRetryMax")) AndAlso _
            CType(ReadAppSetting("CriticalConnectionRetryMax"), Integer) > 0 Then
            DBConnectRetryMax = CType(ReadAppSetting("CriticalConnectionRetryMax"), Integer)
        End If

        'Performance test mode
        If ReadAppSetting("PerformanceTest") = "1" Then
            PerformanceTestMode = True
        End If

    End Sub
    ''' <summary>
    '''     Connect to the vsdVFSImmediateUpdate database to obtain the operating parameters.
    '''     This will try a pre-determined number of times as defined by the app.config file.
    ''' </summary>
    Private Sub ReadDBSettings()

        On Error Resume Next 'start local error handling to handle db connect retries

        Dim intDBConnectAttempt As Integer = 0 'db connect counter
        Dim dsSettings As New DataSet
        Dim r As DataRow
        Dim DBConnectOK As Boolean = False

        Do While DBConnectOK = False
            'get the db app. settings
            dsSettings = DataObject.ExecuteDataset(ConfigValues.ConnectionKey, CommandType.StoredProcedure, _
                "selAppConfig", New SqlClient.SqlParameter("@strProcess", ConfigValues.ProcessMode))

            If Err.Number = 0 Then
                'we were able to connect to the db, so we can retrieve the settings 
                DBConnectOK = True

                'LastEmailSent is initialized as an "old" day upon instantiation 
                'However, if the DB didn't connect on the first try, we may have sent an e-mail so 
                'reset the LastEmailSent value so any new transactions errors generate e-mails immediately
                ConfigValues.LastEmailSent = Now.AddDays(-1)

                On Error GoTo 0 'resume normal error handling. 
                'Any errors here should now bubble up the stack through ServiceRun 
                'to OnStart, log the fatal exception and initiate shutdown

                For Each r In dsSettings.Tables(0).Rows
                    'Me.LogEvent("debug", LCase(CType(r("Name"), String)) & "=" & CType(r("Value"), String), MessageType.Debug, LogType.Standard)
                    Select Case LCase(CType(r("Name"), String))
                        Case "debugmode"
                            ConfigValues.DebugMode = CType(r("Value"), Byte)

                        Case "emailfrequency"
                            ConfigValues.EmailFrequency = CType(r("Value"), Integer)

                        Case "delay"
                            ConfigValues.Delay = CType(r("Value"), Integer) * 1000 'in seconds

                        Case "maxmessageattempts"
                            ConfigValues.MaxProcessAttempts = CType(r("Value"), Integer)

                        Case "proxy"
                            ConfigValues.ProxyName = CType(r("Value"), String)

                        Case "usesystemcredentials"
                            If CType(r("Value"), String) = "0" Then
                                ConfigValues.UseSystemCredentials = False
                            Else
                                'should usually be true
                                ConfigValues.UseSystemCredentials = True
                            End If
                    End Select
                Next

                'get the tran code to web service calls cross-reference
                ConfigValues.TranTypes = DataObject.ExecuteDataset(ConfigValues.ConnectionKey, CommandType.StoredProcedure, _
                    "selTranTypes", New SqlClient.SqlParameter("@strProcess", ConfigValues.ProcessMode)).Tables(0)

                'get the response values 

                'index this datatable so we can do multi-threaded Find calls without fear of conflicts
                Dim Keys(1) As DataColumn

                ConfigValues.ResponseValues = DataObject.ExecuteDataset(ConfigValues.ConnectionKey, CommandType.StoredProcedure, _
                    "selResponseValues", New SqlClient.SqlParameter("@strProcess", ConfigValues.ProcessMode)).Tables(0)

                Keys(0) = ConfigValues.ResponseValues.Columns(0)
                Keys(1) = ConfigValues.ResponseValues.Columns(1)
                ConfigValues.ResponseValues.PrimaryKey = Keys

                Exit Do 'not really necessary since DBConnectOK is now true
            Else
                'Test Case #1
                'error connecting to db; handle retry loop

                'increment our counter
                intDBConnectAttempt += 1

                'log an event (which will send an e-mail, if appropriate)
                LogEvent("ReadDBSettings", "Attempt " & intDBConnectAttempt.ToString & " - " & _
                    Err.Description, MessageType.Error, LogType.Standard)

                If DBConnectRetryMax > 0 AndAlso intDBConnectAttempt >= DBConnectRetryMax Then
                    'we have a DB connect attempt limit and which reached it.

                    On Error GoTo 0 'resume normal error handling. 
                    'Throw exception which should bubble up the stack through ServiceRun 
                    'to OnStart, log the fatal exception, and initiate shutdown.
                    Throw New Exception("Unable to connect to database after " & DBConnectRetryMax.ToString & " attempts.")
                    Exit Sub
                End If

                'sleep for awhile (DBConnectRetryDelay is in seconds, so multiply)
                Thread.Sleep(DBConnectRetryDelay * 1000)

            End If
        Loop ' DBConnectOK = False

    End Sub

    ''' <summary>
    '''     Main thread for the service.
    ''' </summary>
    ''' <param name="State">New thread callback.</param>
    ''' <remarks>
    '''     This runs in a continuous loop until the service is stopped.
    '''     Multiple threads are spawned for each message in the queue, up to the 
    '''     ConfigValue.MaxThreads value. The thread will sleep when no messages are 
    '''     found in the queue or when a recoverable error occurs.
    ''' </remarks>
    Protected Sub ServiceRun(ByVal State As Object)

        'make note that we have started
        LogEvent("ServiceRun", "Checking settings.", MessageType.Start, LogType.Standard)

        Dim Cache As New Hashtable ' memory cache for detecting duplicate threads
        Dim r As DataRow

        Try

            'get the db settings
            ReadDBSettings()

            If ConfigValues.DebugMode > 2 Then
                'give time to attach a debugger
                Thread.Sleep(45000)

            End If

            'validate settings
            If ConfigValues.TranTypes.Rows.Count < 1 Then
                Throw New Exception("No TranTypes found for process [" & ConfigValues.ProcessMode & "]")
            End If

            'make note that we started
            LogEvent("ServiceRun", "Settings ok. Starting main loop.", MessageType.Start, LogType.Standard)

            'set our status to run mode
            ServiceState = ServiceStates.Running

            'set the credentials if present
            If ConfigValues.UseSystemCredentials = True Then
                'use the credentials of the account we are running under
                Credentials = System.Net.CredentialCache.DefaultCredentials()
            End If

            'set the proxy if a name is specified
            If ConfigValues.ProxyName <> "" Then
                Proxy = New System.Net.WebProxy(ConfigValues.ProxyName, True)
                'set the credentials of the proxy if we have credentials
                If Not Credentials Is Nothing Then
                    Proxy.Credentials = Credentials
                End If
            End If

            'spawn one thread per connect string key
            For Each r In ConfigValues.TranTypes.Rows
                If Cache.Contains(LCase(CType(r("ConnectStringKey"), String))) = False Then
                    'add the connect string key to the cache so we only spawn one 
                    'thread for this database
                    Cache.Add(Trim(CType(r("ConnectStringKey"), String)), Trim(CType(r("ConnectStringKey"), String)))

                    'increment the thread count (each thread will decrement this when its done)
                    Interlocked.Increment(ThreadCount)

                    'process each unique connect string key on a separate thread - pass in the TranType to the thread
                    ThreadPool.QueueUserWorkItem(AddressOf ProcessRecords, r("TranType"))
                End If
            Next

            If ThreadCount = 0 Then
                'if no threads were able to start, throw an exception and shutdown
                Throw New Exception("No threads were able to start.")
            End If

        Catch ex As Exception

            'LogEvent, Send E-mail, and quit
            Dim strMessage As String = "Service is unable to proceed. Shutting down. " & ex.Message
            'log the error
            LogEvent("Service_OnStart", strMessage, MessageType.Error, LogType.Standard, True)

            'initiate stop process

            Dim sc As New ServiceController(Me.ServiceName)
            sc.Stop()
            Exit Sub
        End Try
    End Sub
    ''' <summary>
    '''     Common code for updating a record in the source database.
    ''' </summary>
    ''' <param name="ConnectStringKey">Connect string key for the update stored proc</param>
    ''' <param name="SPUpdateName">String name of the update stored proc</param>
    ''' <param name="SPKeyName">Stored proc's string name for the key</param>
    ''' <param name="Key">Record's key value</param>
    ''' <param name="AttemptParam">Stored proc's string name for the attempt counter.</param>
    ''' <param name="IntegrationAttempt">Integer indicating the attempt counter for this record</param>
    ''' <param name="FailedParam">Stored proc's string name for the non-recoverable bit</param>
    ''' <param name="IntegrationFailed">Integer indicating non-recoverable bit</param>
    ''' <remarks>
    '''     DB update occurs in a Try/Catch because if updating the record fails, it could ultimately abort our thread.
    '''     Instead, we log the event and send an e-mail.
    ''' </remarks>
    Private Sub UpdateRecord(ByVal ConnectStringKey As String, _
        ByVal SPUpdateName As String, _
        ByVal SPKeyName As String, _
        ByVal Key As String, _
        ByVal AttemptParam As String, _
        ByVal IntegrationAttempt As Integer, _
        ByVal FailedParam As String, _
        ByVal IntegrationFailed As Integer)

        Try
            DataObject.ExecuteNonQuery(ConnectStringKey, CommandType.StoredProcedure, SPUpdateName, _
                New SqlClient.SqlParameter(SPKeyName, Key), _
                New SqlClient.SqlParameter(AttemptParam, IntegrationAttempt), _
                New SqlClient.SqlParameter(FailedParam, IntegrationFailed))

        Catch ex As Exception
            'log an error and send an e-mail
            Me.LogEvent("UpdateRecord", "Error updating record (SP=" & SPUpdateName & " Key=" & Key & ").", MessageType.Error, _
            LogType.Standard, True)
        End Try
    End Sub

End Class


''' <summary>
'''     This friend class contains all of the operating values required by the service and threads. The
'''     service populates this class once at startup.
''' </summary>
Friend Class ConfigValues

    Private _ConnectionKey As String = "" 'vsdVFSImmediateUpdate connection string key
    Private _DebugMode As Byte = 0 'debugging indicator
    Private _Delay As Integer = 30000 'number of milliseconds to pause the process when a recoverable error occurs
    Private _EmailFrequency As Integer = 900 'number of seconds between error e-mails (db setting updates this value)
    Private _LastEmailSent As Date = Now.AddDays(-1) 'initialize it to an "old" day
    Private _MaxMessageProcessAttempts As Integer = 0 'the number of times a recoverable message should be attempted before notification
    Private _ProcessMode As String = "" 'included to allow multiple instances of the service if necessary
    Private _ProxyName As String = "" 'optional proxy name for the web service calls
    Private _UseSystemCredentials As Boolean = True 'web service security
    
    Private _TranTypes As New DataTable 'table containing TranType cross reference info
    Private _ResponseValues As New DataTable 'table containing response values

    ''' <summary>
    '''     This property sets/returns the vsdVFSImmediateUpdate connection string key.
    ''' </summary>
    Friend Property ConnectionKey() As String
        Get
            Return _ConnectionKey
        End Get
        Set(ByVal Value As String)
            _ConnectionKey = Value
        End Set
    End Property
    ''' <summary>
    '''     This property sets/returns a debug logging value. 0 equals basic debugging. A value of 1 
    '''     equals extra debugging. A value of 2 will also log each message request/response value.
    ''' </summary>
    Friend Property DebugMode() As Byte
        Get
            Return _DebugMode
        End Get
        Set(ByVal Value As Byte)
            _DebugMode = Value
        End Set
    End Property
    ''' <summary>
    '''     This property sets/returns the number of milliseconds the main thread sleeps when a recoverable error occurs or there are no messages to process.
    ''' </summary>
    Friend Property Delay() As Integer
        Get
            Return _Delay
        End Get
        Set(ByVal Value As Integer)
            _Delay = Value
        End Set
    End Property
    ''' <summary>
    '''     This property sets/returns the number of seconds that should pass between e-mail error notifications.
    ''' </summary>
    Friend Property EmailFrequency() As Integer
        Get
            Return _EmailFrequency
        End Get
        Set(ByVal Value As Integer)
            _EmailFrequency = Value
        End Set
    End Property

    ''' <summary>
    '''     This property sets/returns the date that the last e-mail was sent.
    ''' </summary>
    Friend Property LastEmailSent() As Date
        Get
            Return _LastEmailSent
        End Get
        Set(ByVal Value As Date)
            _LastEmailSent = Value
        End Set
    End Property
    ''' <summary>
    '''     This property sets/returns the number of attempts at processing successfully that a single message is given before becoming an exception.
    ''' </summary>
    Friend Property MaxProcessAttempts() As Integer
        Get
            Return _MaxMessageProcessAttempts
        End Get
        Set(ByVal Value As Integer)
            _MaxMessageProcessAttempts = Value
        End Set
    End Property
    ''' <summary>
    '''     This property sets/returns the process name value.
    ''' </summary>
    Friend Property ProcessMode() As String
        Get
            Return _ProcessMode
        End Get
        Set(ByVal Value As String)
            _ProcessMode = Value
        End Set
    End Property
    ''' <summary>
    '''     This property sets/returns the optional proxy name value to use when making the web service calls.
    ''' </summary>
    Friend Property ProxyName() As String
        Get
            Return _ProxyName
        End Get
        Set(ByVal Value As String)
            _ProxyName = Value
        End Set
    End Property
    ''' <summary>
    '''     This property contains the data table of Response values.
    ''' </summary>
    Friend Property ResponseValues() As DataTable
        Get
            Return _ResponseValues
        End Get
        Set(ByVal Value As DataTable)
            _ResponseValues = Value
        End Set
    End Property
    ''' <summary>
    '''     This property contains the data table of TranTypes.
    ''' </summary>
    Friend Property TranTypes() As DataTable
        Get
            Return _TranTypes
        End Get
        Set(ByVal Value As DataTable)
            _TranTypes = Value
        End Set
    End Property
    ''' <summary>
    '''     This property determines whether the web service method is called in the context of the service.
    '''     Normally, this should be True. If False, the web service calls will be anonymous.
    ''' </summary>
    Friend Property UseSystemCredentials() As Boolean
        Get
            Return _UseSystemCredentials
        End Get
        Set(ByVal Value As Boolean)
            _UseSystemCredentials = Value
        End Set
    End Property
    Public Sub New()
    End Sub
End Class
