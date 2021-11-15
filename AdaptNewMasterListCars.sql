SELECT 

sa.Action_Specifics,
cid.CARSID,
CARS = fms.cars, 

fms.Type, 
fms2.FMS_ID, 
fms2.FMS_Name, 
fms.FMS_ORDER, 
fms2.FMS_Transition_Paths,
TransitionPath = fms2.FMS_scenario,
FC_FMS = fms2.FC, 
subProc.Epic_Code,



PLET_FMS = case
				WHEN plet.FMS_ID IS NULL THEN 'FMS not in PLET' else 'Linked to PLET'
		   end,	
				
ProcedureTitle = coalesce(subProc.ProcedureTitle,plet.Title),
ppt.Software,
subProc.MOID,
subProc.Sources, 
ARE_ORDER=subProc.stepCount,
subProc.PROC_ORDER, 
Duration_ARE = subProc.Duration, 
Duration_Coach = ppt.Time, 
Duration_PLET = plet.PLET_Time,

Duration = case 
				when ppt.Time like  subProc.Duration and ppt.time like plet.PLET_Time then 'OK' else 'NOK' 
		   end,


Bottom_up = CASE
				when fms2.FMS_ID is null and fms2.FMS_scenario is null then 0 
				else 
				SUM( cast(SUBSTRING(Coalesce(ppt.Time,subProc.ProposedTime), PATINDEX('%[0-9]%', Coalesce(ppt.Time,subProc.ProposedTime)), 
				PATINDEX('%[0-9][^0-9]%', Coalesce(ppt.Time,subProc.ProposedTime) + 't') - PATINDEX('%[0-9]%', Coalesce(ppt.Time,subProc.ProposedTime)) + 1)  as int ) )
				OVER (PARTITION BY fms.CARS, fms2.FMS_ID, fms2.FMS_scenario) 
			end,

Bottom_up_ARE = CASE
								when fms2.FMS_ID is null and fms2.FMS_scenario is null then 0 
								else
								SUM(subProc.Duration) over (PARTITION BY fms.CARS, fms2.FMS_ID, fms2.FMS_scenario) 
						END,

/*Bottom_up_Worksheets = CASE
								when fms2.FMS_ID is null and fms2.FMS_scenario is null then 0 
								else
								SUM(isnull(cast(subProc.ProposedTime as int),0)) over (PARTITION BY  sa.AVM_ID, fms.CARS, fms2.FMS_ID, fms2.FMS_scenario) 
						END,*/
subProc.ProposedTime,
fms2.Targetspec, 
subProc.Persons, 
subProc.Nesting,
FC_Procedure, 
subProc.Remarks,
plet.KD14,
plet.status_Test_In_Factory,


CSE_Review_Status = CASE
						when plet.status_Test_In_Factory like '1'  then 'CSE reviewed' else 'Not reviewed' end,

FMS_Readiness = CASE 	
						When fms2.FMS_ID is null then 0
						else  sum(case when sources like 'Both' then 1 else 0 end) over (partition by  fms.CARS, fms2.FMS_ID, fms2.FMS_scenario)*100/ 
						count(case when subProc.Epic_Code is not null then 1 else 0 end) over (partition by  fms.CARS, fms2.FMS_ID, fms2.FMS_scenario) 
				end,


plet.tblPro_ID,
plet.System, 
--plet.Milestone, 
plet.Destination, 
plet.Status_Coach, 
plet.Status_Process, 
plet.Status_Safety,
WBS_Description_Procedure,
PL_Procedure,
Responsible_Engineer_Procedure ,	
GL_Procedure ,
DM_Procedure ,
VP_Procedure ,
DepartmentNr,
Ready_Procedure 



 FROM
 --1
 cars.cars_fms fms

 right JOIN  cars.fms fms2 ON fms.fms_id = fms2.fms_id AND fms.transitionpath = fms2.FMS_scenario 

--2

left join [cars].[cars_ID] cid on fms.cars = cid.CARSName

--3
left JOIN [cars].[cars_ActionSpecifics] sa ON sa.CARS_ID = cid.CARSID

--4

--5 to be added
LEFT JOIN 
(
	SELECT 
	MOID = Coalesce(fmsp.MO_ID, SubARE.MOID),
	Duration,
	PROC_ORDER,
	ProcedureTitle,
	ProposedTime,
	Remarks,
	Persons,
	Nesting,
	stepCount,
	FMS_ID = coalesce(fmsp.FMS, subARE.FMS_ID),
	TP_ID = coalesce(fmsp.TP, subARE.TP),
	Epic_Code = coalesce(fmsp.Procedures, subARE.Epic_Code),
	
	
	
	Sources = case when fmsp.FMS is not null and subARE.FMS_ID is not null then 'Both' 
	when fmsp.FMS is null and subARE.FMS_ID is not null then 'ARE only' 
	when fmsp.FMS is not null and subARE.FMS_ID is null then 'Excel only' else 'TBD' end

	FROM 
		(
		SELECT  PROC_ORDER = ROW_NUMBER() over (partition by fms,tp order by Procedure_ORDER),*  FROM cars.fms_procedure fmspSub left join dbo.tblSync_Coach sc on fmspSub.Procedures = sc.Epic_code 
		) fmsp 

	full outer join

		(SELECT MOID,Duration, stepCount, fms.FMS_ID, TP = fms.FMS_scenario, sc.Epic_Code, Source= 'ARE'  FROM 
			(
			select  FMS_Name = ltrim(rtrim(left(substring(Recovery,12,100),charindex(' - ',substring(Recovery,12,100))))), 
				Transition_Path_Name = ltrim(rtrim(right(Recovery,charindex(' - ', reverse(Recovery))))) , MOID, Duration, stepCount
				from [PLET_PRD].[are].[seqFiles] where Type like 3  and Recovery like 'FMS%' and config like '%MV_Field'  --and config like '%3400%'
			)seq
			
			join cars.fms fms on seq.FMS_Name = fms.FMS_name and replace(seq.Transition_Path_Name,'  ', ' ')  =  replace(fms.FMS_Transition_Paths,'  ', ' ') 
			left join dbo.tblSync_Coach sc on seq.MOID = sc.MO_ID
	
		) subARE

		on fmsp.FMS = subARE.FMS_ID and fmsp.TP = subARE.TP and fmsp.Procedures = subARE.Epic_Code
			
	)subProc on fms2.FMS_ID = subProc.FMS_ID and fms2.FMS_scenario = subProc.TP_ID  

left join 

 [pmapp].[vwProcTime] ppt ON subProc.MOID = ppt.MOID and system = 'SOURCE_S3_MV' and destination = 'Service'  
LEFT JOIN (
	
	
	SELECT 
	p.tblPro_ID,
	MOID = p.tblPro_MO_ID,
	System = pm.tblMac_Type,
	Milestone = pmm.tblMil_Milestone,
	Destination = mi.tblMil_COACH_Destination,
	Epic_code = p.tblPro_Epic_Code,		
	Title = p.tblPro_Title,	
	Status_Coach = 
		CASE WHEN p."tblPro_Status_Coach" = 'Not in COACH' THen 'Not in COACH'
		WHEN pm.tblMac_NotInCOACH = 1 THEN 'Not profiled for system' ELSE	p."tblPro_Status_Coach" END,		
	Status_Process = pmm.tblMil_Status, FMS_ID = left(sms.tblSMS_FMS,7), FMS_Name_PLET = substring(sms.tblSMS_FMS,9,100),	 
	Status_Safety = 
		CASE WHEN pmm."tblMil_Status_Safety" Is null OR pmm."tblMil_Status_Safety" ='' then 'Safety not started' else pmm."tblMil_Status_Safety" END,
	Ready_Procedure = CASE WHEN p.tblPro_Status_Coach IN ('Provisional', 'Final') AND pm.tblMac_NotInCOACH = 0 AND pmm.tblMil_Status IN ('R4 testing', 'R4 GL sign off','Released') AND 
	pmm."tblMil_Status_Safety" IN ('Safety signed off','No review needed','6-in-box') THEN 1 ELSE 0 END,	
	WBS_Description_Procedure = pm.tblMac_WBS_Description,
	PL_Procedure = pm.tblMac_PL,
	FC_Procedure = pm.tblMac_FC,
	Responsible_Engineer_Procedure = pmm.tblMil_Responsible_Engineer,	
	GL_Procedure = pmm.tblMil_GL,
	DM_Procedure = vp.DM,
	VP_Procedure = vp.VP,
	DepartmentNr = pmm.tblMil_Org_unit,
	PLET_Time = pmm.tblMil_A_Time,
	KD14 = pmm.tblMil_KD14,
    status_Test_In_Factory = pmm.tblMil_Status_Test_In_Factory

	FROM 

	tblProcedures p  
	left JOIN T_tblProcedures_Machines pm ON p.tblPro_ID = pm.tblPro_ID
 	left JOIN T_tblProcedures_Milestones pmm ON pm.tblPro_ID = pmm.tblPro_ID and pm.tblMac_ID = pmm.tblMac_ID
	left JOIN tblMilestones_Items mi ON pmm.tblMil_ID = mi.tblMil_ID 
	left JOIN dbo.T_tblProcedures_Milestones_SMS sms ON pmm.tblPro_ID = sms.tblPro_ID AND pmm.tblMac_ID = sms.tblMac_ID AND pmm.tblMil_ID = sms.tblMil_ID
	
	left join 
		(SELECT distinct hr1.Organizational_unit, VP = hr2.Abbreviation, DM = hr3.Abbreviation from dbo.tbl_rpt_USERNAME_DATA_ORG_STRUCTURE hr1
		LEFT JOIN dbo.tbl_rpt_USERNAME_DATA_ORG_STRUCTURE hr2 ON hr1.Organizational_unit_L3_manager = hr2.Employee
		LEFT JOIN dbo.tbl_rpt_USERNAME_DATA_ORG_STRUCTURE hr3 ON hr1.Person_manager_L2_up_nr = hr3.Employee) vp
		ON pmm.tblMil_Org_unit = vp.Organizational_unit

	WHERE 
	pm.tblMac_Type = 'SOURCE_S3_MV'  AND 
	mi.tblMil_COACH_Destination = 'Service' 
	or pm.tblMac_Type = 'NXE_3400C'  AND 
	mi.tblMil_COACH_Destination = 'Service'
	
	
	)plet ON  subProc.Epic_Code = plet.Epic_code 


--where subProc.Epic_Code = 'ght016.ins' --fms2.FMS_ID = 'FMS-302' and fms2.FMS_scenario = 1


	
GO
