-- TODO: Change database names when moving to QA
/* 
===================================================================
 AVR STUFF
====================================================================
*/
use vsdAVRAffidavitOfSale
go
-- add the Integration Retry counter
ALTER TABLE buAffidavitOfSale ADD IntegrationAttempt tinyint NULL

-- add the Integration Failed indicator
ALTER TABLE buAffidavitOfSale ADD IntegrationFailed tinyint NULL
go

-- 
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[PAD_RIGHT]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[PAD_RIGHT]
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

/*
	Pad a string to a specific length with a particular padding character

*/
CREATE FUNCTION dbo.PAD_RIGHT(@strIn varchar(255), @intTotalLen tinyint, @strPadChar char(1))
RETURNS varchar(255)
AS
BEGIN
	declare @strResult varchar(255)
 
	if @strIn is Null 
		begin
		        set @strIn = ''
		end
	
	if len(@strIn) < @intTotalLen
		begin
			-- pad right
			Set @strResult = left(@strIn + Replicate(@strPadChar,@intTotalLen),@intTotalLen) 
		end
	else 
		begin
			-- return the left @intTotalLen portion string
			Set @strResult = left(@strIn,@intTotalLen) 
		end

	return ( @strResult )
END



GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[PAD_RIGHT]  TO [public]
GO

GRANT  EXECUTE  ON [dbo].[PAD_RIGHT]  TO [DOL_RSRC\vsdVfsIntServices]
GO





GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[selAVRIntegration]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[selAVRIntegration]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[updAVRIntegration]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[updAVRIntegration]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*
     Name: selAVRIntegration Integration Vehicle Get Records.
  Purpose: Return Records that Need To Be integrated.
    Input: None
   Output: Recordset.
*/
CREATE PROCEDURE dbo.selAVRIntegration
AS
BEGIN

	SET NOCOUNT ON

	select 'AF', -- first four fields are NOT part of the buffer; rather, they are used by the vsdVFSWebServiceIntegration service
	AVSTranNum, 
	coalesce(IntegrationAttempt,0), 
	coalesce(IntegrationFailed,0), 
	'MMRS  ',  -- buffer starts here
	dbo.PAD_RIGHT(Plate,8,' ') , 
	dbo.PADZ(AVSTranNum,10), 
	'66', 
	dbo.DateToCCYYMMDD(DateOfSale), 
	'Y ', 
	dbo.PADZ(AOSNumber,10), 
	dbo.DateToCCYYMMDD(TranDateTime), 
	dbo.DateToHHMMSS(TranDateTime)
	from buAffidavitOfSale
	where (AOSInd IS NULL OR IpcInd IS NULL )   -- Needing to Write to Master or IPC file...
	AND AOSNumber is not null -- old logic
	AND coalesce(IntegrationFailed, 0) = 0 -- must not be marked as failed.

END
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[selAVRIntegration]  TO [DOL_RSRC\vsdVfsIntServices]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*

Name: updAVRIntegration Integration Pushed 
Purpose: Update buAffidavitOfSale to indicate the record was pushed/integrated.

Input:	pstrIntTranKey (Internet Transaction Key) 
	pintIntegrationAttempt - retry counter. 
	pintIntegrationFailed - failed indicator
	
Return: Nothing
  
*/
CREATE PROCEDURE dbo.updAVRIntegration
	@pstrIntTranKey varchar(10),	--* AVSTranNum (Unique).
	@pintIntegrationAttempt int = -1,
	@pintIntegrationFailed int = -1
AS
BEGIN
	set nocount on

	if @pintIntegrationAttempt >=0 or @pintIntegrationFailed >=0
		begin
			-- either we are setting a retry value OR marking the record as "un-integratable" (is that a word?)
			UPDATE buAffidavitOfSale 
			SET IntegrationAttempt = @pintIntegrationAttempt,
			IntegrationFailed = @pintIntegrationFailed
			WHERE AVSTranNum = @pstrIntTranKey
			AND AOSInd IS NULL   -- old logic
			AND IPCInd IS NULL-- old logic

		end
	else
		begin
			-- if both @pintIntegrationCount and @pintIntegrationFailed are -1, transaction is considered integrated
			SET NOCOUNT ON
			
			-- Here is the main goal of what we are trying to do...   Update the PushedToRegion Flag.	
			UPDATE buAffidavitOfSale 
		   	SET AOSInd = GetDate(),
			IpcInd = GetDate(),
			IntegrationAttempt = null,
			IntegrationFailed = null
			WHERE AVSTranNum = @pstrIntTranKey     	      -- Single Transaction
	        		AND AOSInd IS NULL                   -- And Not yet timestamped
			AND IPCInd IS NULL

		end

END

GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[updAVRIntegration]  TO [DOL_RSRC\vsdVfsIntServices]
GO
-- drop old stuff

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationGetConfig]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationGetConfig]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehGetRecs]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehGetRecs]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehIpcCheckout]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehIpcCheckout]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehIpcPushed]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehIpcPushed]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehMasterCheckout]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehMasterCheckout]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehMasterPushed]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehMasterPushed]
GO

/* 
===================================================================
 DV STUFF
====================================================================
*/

use vsdDestroyed
go
-- add the Integration Retry counter
ALTER TABLE buDVIntegration ADD IntegrationAttempt tinyint NULL

-- add the Integration Failed indicator
ALTER TABLE buDVIntegration ADD IntegrationFailed tinyint NULL
go

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[CCYYJJJFromDate]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[CCYYJJJFromDate]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[PAD_LEFT]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[PAD_LEFT]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[PAD_RIGHT]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[PAD_RIGHT]
GO

SET QUOTED_IDENTIFIER ON 
GO
SET ANSI_NULLS ON 
GO

CREATE FUNCTION [dbo].[CCYYJJJFromDate](@dtDate datetime)  
RETURNS char(7)
AS  

BEGIN 

declare @ReturnValue as char(7)

if @dtDate is null
	begin
		set @ReturnValue = '0000000'
	end
else
	begin
		set @ReturnValue = dbo.PADZ(Year(@dtDate), 4) + dbo.PADZ(DateDiff(d, cast('01/01/' + dbo.PADZ(Year(@dtDate), 4) as datetime), @dtDate) + 1, 3)
	end

return @ReturnValue

END


GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[CCYYJJJFromDate]  TO [public]
GO

GRANT  EXECUTE  ON [dbo].[CCYYJJJFromDate]  TO [DOL_RSRC\vsdVfsIntServices]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO



/*
	Pad a string a to a specific length with a particular padding character

*/
CREATE FUNCTION dbo.PAD_LEFT(@strIn varchar(255), @intTotalLen tinyint, @strPadChar char(1))
RETURNS varchar(255)
AS
BEGIN
	declare @strResult varchar(255)
 
	if @strIn is Null 
		begin
		        set @strIn = ''
		end
	
	if len(@strIn) < @intTotalLen
		begin
			-- pad left
			Set @strResult = right(Replicate(@strPadChar,@intTotalLen)+ @strIn,@intTotalLen) 
		end
	else 
		begin
			-- return the left @intTotalLen portion string
			Set @strResult = left(@strIn,@intTotalLen) 
		end

	return ( @strResult )
END


GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[PAD_LEFT]  TO [public]
GO

GRANT  EXECUTE  ON [dbo].[PAD_LEFT]  TO [DOL_RSRC\vsdVfsIntServices]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO


/*
	Pad a string to a specific length with a particular padding character

*/
CREATE FUNCTION dbo.PAD_RIGHT(@strIn varchar(255), @intTotalLen tinyint, @strPadChar char(1))
RETURNS varchar(255)
AS
BEGIN
	declare @strResult varchar(255)
 
	if @strIn is Null 
		begin
		        set @strIn = ''
		end
	
	if len(@strIn) < @intTotalLen
		begin
			-- pad right
			Set @strResult = left(@strIn + Replicate(@strPadChar,@intTotalLen),@intTotalLen) 
		end
	else 
		begin
			-- return the left @intTotalLen portion string
			Set @strResult = left(@strIn,@intTotalLen) 
		end

	return ( @strResult )
END


GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[PAD_RIGHT]  TO [public]
GO

GRANT  EXECUTE  ON [dbo].[PAD_RIGHT]  TO [DOL_RSRC\vsdVfsIntServices]
GO



if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[selDVIntegration]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[selDVIntegration]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[updDVIntegration]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[updDVIntegration]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*
     Name: selDVIntegration Integration Vehicle Get Records.
  Purpose: Return Records that Need To Be integrated.
    Input: None
   Output: Recordset.
*/
CREATE PROCEDURE dbo.selDVIntegration
AS
BEGIN

	SET NOCOUNT ON

	select 'AX', -- first four fields are NOT part of the buffer; rather, they are used by the vsdVFSWebServiceIntegration service
	DestVehTranNum,
	coalesce(IntegrationAttempt,0), 
	coalesce(IntegrationFailed,0), 
	'MMDEST',  -- buffer starts here
	dbo.PAD_LEFT(dbo.CCYYJJJFromDate(TranDate), 8, ' '),
	dbo.PADZ(TranTime,6),
	dbo.PAD_RIGHT(TranCode,2,' '), 
	dbo.PAD_RIGHT(Plate,7,' '), 
	dbo.PAD_RIGHT(VIN,17,' '), 
	dbo.PADZ(RebuiltInd,2),
	dbo.PAD_RIGHT(DestroyDate,6,' '), 
	dbo.PADZ(DealerNum,4)
	from buDestroyedVehicle 
	WHERE  ( DstInd IS NULL OR IpcInd IS NULL )     -- Needing to Write to Master or IPC file...
	AND coalesce(IntegrationFailed, 0) = 0 -- must not be marked as failed.

END
GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[selDVIntegration]  TO [DOL_RSRC\vsdVfsIntServices]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*

Name: updDVIntegration Integration Pushed 
Purpose: Update buDVIntegration to indicate the record was pushed/integrated.

Input:	pstrIntTranKey (Internet Transaction Key) 
	pintIntegrationAttempt - retry counter. 
	pintIntegrationFailed - failed indicator
	
Return: Nothing
  
*/
CREATE PROCEDURE dbo.updDVIntegration
	@pstrIntTranKey varchar(10),	--* DestVehTranNum (Unique).
	@pintIntegrationAttempt int = -1,
	@pintIntegrationFailed int = -1
AS
BEGIN
	set nocount on

	if @pintIntegrationAttempt >=0 or @pintIntegrationFailed >=0
		begin
			-- either we are setting a retry value OR marking the record as "un-integratable" (is that a word?)
			UPDATE buDVIntegration 
			SET IntegrationAttempt = @pintIntegrationAttempt,
			IntegrationFailed = @pintIntegrationFailed
			WHERE DestVehTranNum = @pstrIntTranKey
			AND DstInd IS NULL   -- old logic
			AND IPCInd IS NULL-- old logic

		end
	else
		begin
			-- if both @pintIntegrationCount and @pintIntegrationFailed are -1, transaction is considered integrated
			SET NOCOUNT ON
			
			-- Here is the main goal of what we are trying to do...   Update the PushedToRegion Flag.	
			UPDATE buDVIntegration 
		   	SET DstInd = GetDate(),
			IpcInd = GetDate(),
			IntegrationAttempt = null,
			IntegrationFailed = null
			WHERE DestVehTranNum = @pstrIntTranKey     	      -- Single Transaction
	        		AND DstInd IS NULL                   -- And Not yet timestamped
			AND IPCInd IS NULL

		end

END

GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[updDVIntegration]  TO [DOL_RSRC\vsdVfsIntServices]
GO

-- drop old stuff
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationGetConfig]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationGetConfig]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehGetRecs]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehGetRecs]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehIpcCheckout]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehIpcCheckout]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehIpcPushed]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehIpcPushed]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehMasterCheckout]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehMasterCheckout]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[batHpIntegrationVehMasterPushed]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[batHpIntegrationVehMasterPushed]
GO


/* 
===================================================================
 ROS STUFF
====================================================================
*/

use vsROSOnline
go
-- add the Integration Retry counter
ALTER TABLE tblROSTransactions ADD IntegrationAttempt tinyint NULL

-- add the Integration Failed indicator
ALTER TABLE tblROSTransactions ADD IntegrationFailed tinyint NULL
go

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[PAD_RIGHT]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[PAD_RIGHT]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO


/*
	Pad a string to a specific length with a particular padding character

*/
CREATE FUNCTION dbo.PAD_RIGHT(@strIn varchar(255), @intTotalLen tinyint, @strPadChar char(1))
RETURNS varchar(255)
AS
BEGIN
	declare @strResult varchar(255)
 
	if @strIn is Null 
		begin
		        set @strIn = ''
		end
	
	if len(@strIn) < @intTotalLen
		begin
			-- pad right
			Set @strResult = left(@strIn + Replicate(@strPadChar,@intTotalLen),@intTotalLen) 
		end
	else 
		begin
			-- return the left @intTotalLen portion string
			Set @strResult = left(@strIn,@intTotalLen) 
		end

	return ( @strResult )
END

GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO


GRANT  EXECUTE  ON [dbo].[PAD_RIGHT]  TO [public]
GO

GRANT  EXECUTE  ON [dbo].[PAD_RIGHT]  TO [DOL_RSRC\vsdVfsIntServices]
GO


if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[DateFromCCYYMMDD]') and xtype in (N'FN', N'IF', N'TF'))
drop function [dbo].[DateFromCCYYMMDD]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO



CREATE FUNCTION dbo.DateFromCCYYMMDD  (@strDate varchar(8))
RETURNS datetime
AS
BEGIN
Declare @dtResult datetime

--** If Day is missing, Set to 01
if len(@strDate)=6 
  Begin
	Set @strDate = @strDate + '01'
  End

if isDate(@strDate) = 1
  Begin
	Set @dtResult = Cast(@strDate as datetime) 
  End
Else
  Begin
	Set @dtResult = Null
  End
  RETURN(@dtResult)
END

GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO


GRANT  EXECUTE  ON [dbo].[DateFromCCYYMMDD]  TO [public]
GO

GRANT  EXECUTE  ON [dbo].[DateFromCCYYMMDD]  TO [DOL_RSRC\vsdVfsIntServices]
GO


if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[selROSIntegration]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[selROSIntegration]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[selROSIntegrationToVFS]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[selROSIntegrationToVFS]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[updROSIntegration]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[updROSIntegration]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[updROSIntegrationToVFS]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[updROSIntegrationToVFS]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*
     Name: selROSIntegration
	   Integration Vehicle Get Records.
  Purpose: Return ROS Vehicle Records that Need To Be integrated via the web service integration (i.e. - called by the 
	vsdVFSWebServiceIntegration service).
    Input: None
   Output: Recordset.

Remarks: the vsdVFSDatabaseIntegration service also integrates the records to the Field System database independently.
	
*/
CREATE PROCEDURE dbo.selROSIntegration
AS
BEGIN

	SET NOCOUNT ON

	SELECT  'AS', -- first four fields are NOT part of the buffer; rather, they are used by the vsdVFSWebServiceIntegration service
	IntTranKey,
	coalesce(IntegrationAttempt,0), 
	coalesce(IntegrationFailed,0), 
	'MMRS  ', -- buffer starts here
	dbo.PAD_RIGHT(Plate,8,' ') , 
	dbo.PAD_RIGHT(IntTranKey,10,' '), 
	'  ', 
	dbo.DateToCCYYMMDD(ROSDate), 
	'Y ', 
	dbo.PADZ(ROSNumber,10), 
	dbo.DateToCCYYMMDD(TranDate), 
	dbo.PADZ(dbo.DateToHHMMSS(TranDate),6)
	FROM tblROSTransactions (nolock)
	WHERE  IPCInd = 0
	AND coalesce(IntegrationFailed, 0) = 0 -- must not be marked as failed.

END
GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[selROSIntegration]  TO [DOL_RSRC\vsdVfsIntServices]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*
     Name: selROSIntegrationToVFS
	   Integration Vehicle Get Records for the VFS.
  Purpose: Return Vehicle Records that 
	a) have been successfully WiniSys integrated via the vsdVFSWebService integration service 

	AND

	b) need to be integrated to the Field System via the vsdVFSDatabaseIntegration service.

    Input: None
   Output: Recordset.

Remarks: the vsdVFSWebServiceIntegration service integrates the records to the WiniSys system database 
	independently and PRIOR TO the Field System integration.

*/
CREATE PROCEDURE dbo.selROSIntegrationToVFS
AS
BEGIN

	SET NOCOUNT ON

select 	
	[IntTranKey] = IntTranKey,		-- key used by the DatabaseIntegration service for updating 
	[DeliveryKey] = '',			-- placeholder (but ignored by) the DatabaseIntegration service
	[@TranType] = 'AS', 			--'AS' -> [varchar] (2) NOT NULL ,
	[@Batch] = 0, 				-- 0 -> [decimal](4, 0) NULL ,
	[@BatchType] = '', 			-- '' -> [varchar] (2) NOT NULL ,
	[@TranKey] = IntTranKey, 		-- [char] (10) -> [varchar] (10) NOT NULL ,
	[@Plate] = RTRIM(Plate), 		-- [char] (7) -> [varchar] (8) NOT NULL ,
	[@ModelYear] = RTRIM(ModelYrVh),	-- [char] (4) -> [decimal](4, 0) NULL ,
	[@Make] = RTRIM(Make), 			-- [char] (5) -> [varchar] (6) NOT NULL ,
	[@VIN] = RTRIM(VIN), 			-- [char] (17) -> [varchar] (18) NOT NULL ,
	[@TitleNumber] = RTRIM(TitleNo),	-- [char] (10) -> [decimal](10, 0) NULL ,
	[@TransferDate] = dbo.DateFromCCYYMMDD(TransferDate), -- [char] (8) -> [datetime] NULL ,
	[@FinalOperator] = 0, 			-- 0 -> [decimal](2, 0) NULL ,
	[@FinalWorkstation] = 0, 		-- 0 -> [decimal](2, 0) NULL ,
	[@SellerName1] = SellerName1, 		-- [varchar] (30) -> [varchar] (30) NOT NULL ,
	[@SellerName2] = '', 			-- '' -> [varchar] (30) NOT NULL ,
	[@SellerName3] = '', 			-- '' -> [varchar] (30) NOT NULL ,
	[@SellerAddress1] = SellerAddr1, 	-- [varchar] (30) -> [varchar] (30) NOT NULL ,
	[@SellerAddress2] = SellerAddr2, 	-- [varchar] (30) -> [varchar] (30) NOT NULL ,
	[@SellerCity] = SellerCity, 		-- [varchar] (20) -> [varchar] (20) NOT NULL ,
	[@SellerState] = SellerState, 		-- [char] (2) -> [varchar] (2) NOT NULL ,
	[@SellerZip] = RTRIM(SellerZip), 	-- [char] (5) -> [varchar] (6) NOT NULL ,
	[@SellerZip4] = RTRIM(SellerZip4),	-- [char] (4) -> [varchar] (4) NOT NULL ,
	[@PurchaserName1] = PurchName1, 	-- [varchar] (30) -> [varchar] (30) NOT NULL ,
	[@PurchaserName2] = '', 		-- '' -> [varchar] (30) NOT NULL ,
	[@PurchaserAddress1] = PurchAddr1, 	-- [varchar] (30) -> [varchar] (30) NOT NULL ,
	[@PurchaserAddress2] = PurchAddr2, 	-- [varchar] (30) -> [varchar] (30) NOT NULL ,
	[@PurchaserCity] = PurchCity, 		-- [varchar] (20) -> [varchar] (20) NOT NULL ,
	[@PurchaserState] = PurchState, 	-- [char] (2) -> [varchar] (2) NOT NULL ,
	[@PurchaserZip] = RTRIM(PurchZip), 	-- [char] (5) -> [varchar] (6) NOT NULL ,
	[@PurchaserZip4] = RTRIM(PurchZip4), 	-- [char] (4) -> [varchar] (4) NOT NULL ,
	[@PurchasePrice] = UsePurPrice, 	-- [int] -> [int] NULL ,
	[@ReportOfSaleDate] = ROSDate, 		-- [smalldatetime] -> [datetime] NULL ,
	[@ReportOfSaleFlag] = 'Y', 		-- 'Y' [varchar] (2) NOT NULL ,
	[@ReportOfSaleNumber] = RTRIM(ROSNumber), -- [char] (9) -> [decimal](10, 0) NULL ,
	[@FilingFee] = 0, 			-- 0 -> [decimal](5, 2) NULL ,
	[@SubagentFee] = 0, 			-- 0 -> [decimal](6, 2) NULL ,
	[@LicenseServiceFee] = 0, 		-- 0 -> [decimal](5, 2) NULL ,
	[@TotalFee] = 0, 			-- 0 -> [decimal](8, 2) NULL ,
	[@CheckPaid] = 0, 			-- 0 -> [decimal](9, 2) NULL ,
	[@CashPaid] = 0, 			-- 0 -> [decimal](9, 2) NULL ,
	[@StatusFlag] = 'F', 			-- 'F' -> [varchar] (2) NOT NULL ,
	[@PrintSequenceNumber] = 0, 		-- 0 -> [decimal](4, 0) NULL ,
	[@TranDate] = TranDate, 		-- [datetime] -> [datetime] NULL ,
	[@CurrentTime] = dbo.DateToHHMMSS(GetDate()), -- GetDate() -> [decimal](6, 0) NULL ,
	[@Dealer] = RTRIM(Dealer), 		-- [char] (4) -> [decimal](4, 0) NULL ,
	[@BatchSequenceNumber] = 0, 		-- 0 -> [decimal](2, 0) NULL ,
	[@ReportOfSaleReasonCode] = '', 	-- '' -> [varchar] (2) NOT NULL ,
	[@UpdateStatus] = 'U', 			-- 'U' -> [varchar] (2) NOT NULL ,
	[@SeriesBody] = SerBody, 		-- [char] (8) -> [varchar] (8) NOT NULL ,
	[@Use] = [Use], 			-- [char] (3) -> [varchar] (4) NOT NULL ,
	[@TranCode] = '', 			-- '' -> [varchar] (2) NOT NULL ,
	[@StolenIndicator] = StolenInd, 	-- [char] (2) -> [varchar] (2) NOT NULL ,
	[@Power] = [Power], 			-- [char] (1) -> [varchar] (2) NOT NULL ,
	[@GWT] = RTRIM(GWT) 			-- [char] (6) -> [decimal](6, 0) NULL ,
	FROM tblROSTransactions (nolock)
	WHERE  IPCInd = 1 			-- web service integration was successful
	and RptSaleInd=0			-- not yet copied to Field System 
	AND coalesce(IntegrationFailed, 0) = 0 	-- must not be marked as web service failed.
END
GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[selROSIntegrationToVFS]  TO [DOL_RSRC\vsdVfsIntServices]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*

Name: 	updROSIntegration

Purpose: Update tblROSTransactions.IpcInd to 1. 	This indicates that it was Written to web service.
	Used by the vsdVFSWebServiceIntegration service 
	after the web service integration occurs. 

Input:	pstrIntTranKey (Internet Transaction Key) 
	pintIntegrationAttempt - retry counter. 
	pintIntegrationFailed - failed indicator
	
Return: Nothing

Remarks: the vsdVFSDatabaseIntegration service integrates the records to the Field System database independently.
  
*/
CREATE PROCEDURE dbo.updROSIntegration
	@pstrIntTranKey varchar(10),	--* IntTranKey (Unique).
	@pintIntegrationAttempt int = -1,
	@pintIntegrationFailed int = -1
AS
BEGIN
	set nocount on

	if @pintIntegrationAttempt >=0 or @pintIntegrationFailed >=0
		begin
			-- either we are setting a retry value OR marking the record as "un-integratable" (is that a word?)
			UPDATE tblROSTransactions 
			SET IntegrationAttempt = @pintIntegrationAttempt,
			IntegrationFailed = @pintIntegrationFailed
			WHERE IntTranKey = @pstrIntTranKey
			AND IpcInd = 0 -- old logic
			-- AND RptSaleInd = 0 -- old logic

		end
	else
		begin
			-- if both @pintIntegrationCount and @pintIntegrationFailed are -1, transaction is considered integrated
			SET NOCOUNT ON
			
			-- Here is the main goal of what we are trying to do...   Update the PushedToRegion Flag.	
			UPDATE tblROSTransactions 
			SET IpcInd = 1,
			IntegrationAttempt = null,
			IntegrationFailed = null
			WHERE IntTranKey = @pstrIntTranKey
			AND IpcInd = 0 -- old logic
			-- AND RptSaleInd = 0 -- old logic
		end
END
GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[updROSIntegration]  TO [DOL_RSRC\vsdVfsIntServices]
GO

SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS OFF 
GO

/*

Name: updROSIntegrationToVFS HP3000 Integration Vehicle Pushed 
Purpose: Update tblROSTransactions.IpcInd to 1.
	This indicates that it was Written to IPC (Message) File.

Input:	pstrIntTranKey (Internet Transaction Key) 
	pintIntegrationAttempt - retry counter. 
	pintIntegrationFailed - failed indicator
	
Return: Nothing
  
*/
CREATE PROCEDURE dbo.updROSIntegrationToVFS
	@pstrIntTranKey varchar(10),	--* IntTranKey (Unique).
	@pdatStartTime datetime -- interface stat start time - not used for ROS
AS
BEGIN
	set nocount on

	
	-- Here is the main goal of what we are trying to do...   Update the PushedToRegion Flag.	
	UPDATE tblROSTransactions 
	SET RptSaleInd = 1
	WHERE IntTranKey = @pstrIntTranKey
	AND IpcInd = 1 -- old logic
	AND RptSaleInd = 0 -- old logic
	
END
GO
SET QUOTED_IDENTIFIER OFF 
GO
SET ANSI_NULLS ON 
GO

GRANT  EXECUTE  ON [dbo].[updROSIntegrationToVFS]  TO [DOL_RSRC\vsdVfsIntServices]
GO



-- drop old stuff
if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_HPInt_CheckOutInit]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_HPInt_CheckOutInit]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_HPInt_Veh_Checkout]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_HPInt_Veh_Checkout]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_HPInt_Veh_MsgCheckout]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_HPInt_Veh_MsgCheckout]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_HpInt_CheckOutExit]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_HpInt_CheckOutExit]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_HpInt_Veh_GetRecs]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_HpInt_Veh_GetRecs]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_HpInt_Veh_MsgPushed]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_HpInt_Veh_MsgPushed]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[usp_HpInt_Veh_Pushed]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)
drop procedure [dbo].[usp_HpInt_Veh_Pushed]
GO

if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[v_REGNDB_M_RPT_SALE]') and OBJECTPROPERTY(id, N'IsView') = 1)
drop view [dbo].[v_REGNDB_M_RPT_SALE]
GO