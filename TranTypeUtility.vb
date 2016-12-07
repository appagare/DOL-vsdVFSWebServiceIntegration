Option Strict On
Option Explicit On 

''' <summary>
'''     This is a helper class that handles all of the TranType specific tasks.
''' </summary>
Friend Class TranTypeUtility
    Private DataObject As WA.DOL.Data.SqlHelper 'common Data object
    Private _TranType As String = ""
    Private _TranTypes As New DataTable
    Private _Index As Byte = 0
    Private _ConnectStringKey As String = ""
    Private _SPSelectName As String = ""
    Private _SPUpdateName As String = ""
    Private _SPKeyName As String = ""
    
    ''' <summary>
    '''     Returns the number of web service calls for the current tran code
    ''' </summary>
    Friend ReadOnly Property CallCount() As Byte
        Get
            Dim ReturnValue As Byte = 0
            If _TranType <> "" Then
                ReturnValue = CType(_TranTypes.DefaultView.Count, Byte)
            End If
            Return ReturnValue
        End Get
    End Property
    ''' <summary>
    '''     Returns the ConnectStringKey set by the New constructor.
    ''' </summary>
    Friend ReadOnly Property ConnectStringKey() As String
        Get
            Return _ConnectStringKey
        End Get
    End Property
    ''' <summary>
    '''     Sets/returns the index for the web service calls
    ''' </summary>
    Friend Property Index() As Byte
        Get
            Return _Index
        End Get
        Set(ByVal Value As Byte)
            _Index = Value
        End Set
    End Property
    ''' <summary>
    '''     Returns the WebService's response length based on the TranType and Index.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    Friend ReadOnly Property ResponseLength(ByVal Index As Integer) As Integer
        Get
            If CallCount > 0 Then
                Return CType(_TranTypes.DefaultView(Index)("ResponseLength"), Integer)
            Else
                Return 0
            End If
        End Get
    End Property
    ''' <summary>
    '''     Returns the WebService's response starting position based on the TranType and Index.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    Friend ReadOnly Property ResponseOffset(ByVal Index As Integer) As Integer
        Get
            If CallCount > 0 Then
                Return CType(_TranTypes.DefaultView(Index)("ResponseOffset"), Integer)
            Else
                Return 0
            End If
        End Get
    End Property
    ''' <summary>
    '''     Returns the common SPKeyName set by the New constructor.
    ''' </summary>
    Friend ReadOnly Property SPKeyName() As String
        Get
            Return _SPKeyName
        End Get
    End Property
    ''' <summary>
    '''     Returns the common SPSelName set by the New constructor.
    ''' </summary>
    Friend ReadOnly Property SPSelectName() As String
        Get
            Return _SPSelectName
        End Get
    End Property
    ''' <summary>
    '''     Returns the common SPUpdName set by the New constructor.
    ''' </summary>
    Friend ReadOnly Property SPUpdateName() As String
        Get
            Return _SPUpdateName
        End Get
    End Property
    ''' <summary>
    '''     Returns True if the tran code exists in the table.
    ''' </summary>
    ''' <remarks>
    '''     This function should be called before trying to reference any of the TranType's properties
    '''     (i.e. - URL(), WebMethod(), etc.)
    ''' </remarks>
    Friend Function TranTypeIsValid(ByVal TranType As String) As Boolean
        If TranType <> _TranType Then
            'make sure the record's trantype matches the tran type supported by this key
            Return False
        End If
        Try
            If Me.CallCount > 0 Then
                'at least one row exists for tran code, so return True
                Return True
            Else
                'return false if a matching URL and method is not found for the TranType
                Return False
            End If
        Catch ex As Exception
            'exception would occur if tran code table is empty (which should've been caught in the contructor)
            Return False
        End Try
    End Function
    ''' <summary>
    '''     Returns the TranType passed into the Constructor.
    ''' </summary>
    Friend ReadOnly Property TranType() As String
        Get
            Return _TranType
        End Get
    End Property
    ''' <summary>
    '''     Returns the WebService's URL based on the TranType and Index.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    Friend ReadOnly Property URL(ByVal Index As Integer) As String
        Get
            If CallCount > 0 Then
                Return CType(_TranTypes.DefaultView(Index)("URL"), String)
            Else
                Return ""
            End If
        End Get
    End Property
    ''' <summary>
    '''     Returns the WebService's Method based on the TranType and Index.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1.</param>
    Friend ReadOnly Property WebMethod(ByVal Index As Integer) As String
        Get
            If CallCount > 0 Then
                Return CType(_TranTypes.DefaultView(Index)("Method"), String)
            Else
                Return ""
            End If
        End Get
    End Property

    ''' <summary>
    '''     Returns True if the "status" attribute is greater than the Index, meaning the call was completed. Otherwise, returns false.
    ''' </summary>
    ''' <param name="Index">Record index between zero and record count - 1 indicating a particular web method call.</param>
    Friend Function IsProcessCallCompleted(ByVal Index As Byte) As Boolean
        'Reserved for Online Update - until then, always returns False
        Return False
    End Function

    Public Sub New(ByVal TranType As String, ByVal TranTypes As DataTable)
        'pass in the TranType table and filter on the desired tran code
        _TranTypes = CopyDatatable(TranTypes)
        _TranType = TranType
        _TranTypes.DefaultView.RowFilter = "TranType='" & TranType & "'"
        If _TranTypes.DefaultView.Count < 1 Then
            Throw New Exception("Unable to find TranType '" & TranType & "' in list.")
        End If
        _ConnectStringKey = CType(_TranTypes.DefaultView(0)("ConnectStringKey"), String)
        _SPSelectName = CType(_TranTypes.DefaultView(0)("SPSelName"), String)
        _SPUpdateName = CType(_TranTypes.DefaultView(0)("SPUpdName"), String)
        _SPKeyName = CType(_TranTypes.DefaultView(0)("SPKeyParamName"), String)

    End Sub
    Private Function CopyDatatable(ByVal SrcTable As DataTable) As DataTable
        'create a true copy of the datatable so it behaves properly as a object passed ByVal
        Dim DstTable As New DataTable
        Dim SrcRow As DataRow
        Dim SrcCol As DataColumn

        DstTable = SrcTable.Clone
        For Each SrcRow In SrcTable.Rows
            Dim DstRow As DataRow
            DstRow = DstTable.NewRow()
            For Each SrcCol In SrcTable.Columns
                DstRow(SrcCol.ColumnName) = SrcRow(SrcCol.ColumnName)
            Next
            DstTable.Rows.Add(DstRow)
        Next
        'return 
        Return DstTable

    End Function

    Protected Overrides Sub Finalize()
        MyBase.Finalize()
    End Sub
End Class
