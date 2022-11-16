SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

USE data_pool

SELECT SLE
	,ltrim(rtrim(SourceForCalcs.EquipmentID)) EquipmentID
	,Desc1
	,EquipPL
	,CompPL
	,ItemID
	,ItemDesc
	,Partslocation
	,tbl_SSTOK.OrderYNPDXAM AS [Frozen?]
	,QtyUnitsUsed
	,Crew
	,QtyUnitsUsed / SLE AS ConsumptionRate_perLRU
	,Tbl_SSTOK.AvgUsage
	,tbl_SSTOK.LocationNumber
	,tbl_SSTOK.QuantityOnHand AS [Qty OH]
	,tbl_SSTOK.QuantityCommitted AS [Qty Committed]
	,tbl_SSTOK.QtyOnRequisition AS [Qty on Order]
	,tbl_SSTOK.QtyOnBackorder AS [Qty on Backorder]
	,tbl_SSTOK.AverageCost
	,VendorName
	,vendorNumber
	,CASE 
		WHEN AvgLT_PrimaryVendorOnly IS NOT NULL
			THEN AvgLT_PrimaryVendorOnly
		WHEN AvgLT_PrimaryVendorOnly IS NULL
			THEN AVGLT
		END AS Average_LT
--Get Lines
FROM (
	SELECT SUM(QtyUnits) AS QtyUnitsUsed
		,PartsLocation
		,HeaderData.EquipmentID
		,ItemID
		,MAX(ItemDesc) AS ItemDesc
		,MAX(Desc1) Desc1
		,MAX(Crew) AS Crew
		,MAX(EquipPL) AS EquipPL
		,MAX(CompPL) AS CompPL
	FROM (
		SELECT QtyUnits
			,PartsLocation
			,SvSOdetsonbr
			,ItemID
			,Desc1 AS ItemDesc
		FROM tbl_SVO_LineData
		WHERE (tbl_SVO_LineData.PartsSerialNo IS NOT NULL)
			AND (tbl_SVO_LineData.PartsSerialNo <> '')
		) LineData
	--Retrive Header Info
	JOIN (
		SELECT MasterSVO
			,ShippedDate
			,EquipmentID
			,Desc1
			,Crew
		FROM tbl_SVO_MasterData
		--ConsumptionData based on specific events
		WHERE SvcOrderType IN (
				'REP'
				,'OVH'
				,'TST'
				)
			AND Cast(ShippedDate AS DATETIME) >= dateadd(month, datediff(month, 0, getdate()) - 12, 0)
			AND InternalCustomer = 'N'
		) HeaderData ON HeaderData.MasterSVO = LineData.SvSoDetSoNbr
	--Get Product Lines for line items
	LEFT JOIN (
		SELECT DISTINCT MAX(PRODUCTLINE) AS CompPL
			,StockID
		FROM tbl_xd035_ls
		GROUP BY StockID
		) CompPL ON LineData.ItemID = CompPL.StockID
	--Get Product Line for LRU
	LEFT JOIN (
		SELECT DISTINCT RecordKey
			,MAX(ProductLine) AS EquipPL
		FROM tbl_VX010_LS
		GROUP BY RecordKey
		) EquipPL ON HeaderData.EquipmentID = EquipPL.RecordKey
	GROUP BY ItemID
		,PartsLocation
		,EquipmentID
	) SourceForCalcs
LEFT JOIN (
	SELECT COUNT(*) AS SLE
		,EquipmentID
	FROM tbl_SVO_MasterData
	WHERE SvcOrderType IN (
			'REP'
			,'OVH'
			,'TST'
			)
		AND Cast(ShippedDate AS DATETIME) >= dateadd(month, datediff(month, 0, getdate()) - 12, 0)
	GROUP BY EquipmentID
	) SLECalc ON SourceForCalcs.EquipmentID = SLECalc.EquipmentID
--FLOORSTOCK TEMPORARILY REMOVED FOR THE SHORT TERM, 012 ADDED	
LEFT JOIN (
	SELECT DISTINCT S.LocationNumber
		,S.StockNumber
		,S.QuantityOnHand
		,S.QuantityCommitted
		,S.QtyOnRequisition
		,S.QtyOnBackorder
		,S.AverageCost
		,S.AvgUsage
		,S.OrderYNPDXAM
		,tbl_AVEND.VendorName
		,tbl_AVEND.VendorNumber
		,dbo.tbl_AverageLT.AvgLT_PrimaryVendorOnly
		,dbo.tbl_AverageLT.AVGLT
	FROM dbo.tbl_SSTOK AS S
	INNER JOIN dbo.tbl_AVEND ON S.PrimaryVendorNo = dbo.tbl_AVEND.VendorNumber
	LEFT OUTER JOIN dbo.tbl_AverageLT ON S.StockNumber = dbo.tbl_AverageLT.StockID
		AND S.LocationNumber = dbo.tbl_AverageLT.Location /*data for avg lt*/
	WHERE S.LocationNumber IN (
			'001'
			,'002'
			,'003'
			,'012' /*,'411','421','431', '432','441'*/
			)
		AND ltrim(rtrim(S.LocationNumber)) IS NOT NULL
		AND ltrim(rtrim(S.LocationNumber)) <> ''
	) AS tbl_SSTOK ON SourceForCalcs.ItemID = tbl_SSTOK.StockNumber
	AND SourceForCalcs.PartsLocation = tbl_SSTOK.LocationNumber /*and tbl_SSTOK.VendorName = tbl_AVEND.VendorName*/
WHERE tbl_SSTOK.LocationNumber IN (
		'001'
		,'002'
		,'003'
		,'012' /*, '411','421','431', '432','441'*/
		)
	AND ltrim(rtrim(tbl_SSTOK.LocationNumber)) IS NOT NULL
	AND ltrim(rtrim(tbl_SSTOK.LocationNumber)) <> ''
	AND SourceForCalcs.EquipmentID = ?
ORDER BY EquipmentID
	,ItemID
	,PartsLocation DESC

