/********************************************************
* UDW INNOVATION DAYS 2024
* July 15, 2024 - Day 1
* Building Analytic Datasets with ClearScape Analytics
* -------------------------------------------------------
* INSTRUCTIONS
* 1. Use your favorite SQL editing tool
* 2. Connect to UDWTEST
* 3. Follow along with the demo
********************************************************/

-- Set your default database
DATABASE INOUDWTRAINING2024;

-- Review the patients table and check number of rows
SELECT * FROM patients pat SAMPLE 3;
SELECT count(*) as how_many FROM patients pat;
-- How many? Answer: 

-- Review the encounters table and check number of rows
SELECT * FROM encounters enc SAMPLE 3;
SELECT count(*) as how_many FROM encounters enc
-- How many? Answer: 

-- Review the encounters table and check number of rows
SELECT * FROM conditions con SAMPLE 3;
SELECT count(*) as how_many FROM conditions con 
-- How many? Answer: 

CALL val.td_analyze (
    'values',
    'database = INOUDWTRAINING2024;
    tablename = conditions;
    columns=all;
    outputdatabase=INOUDWTRAINING2024;
    outputtablename=v_conditions;
    overwrite=true;
    uniques=true;');

SELECT xcol, xtype, 
    CAST(xcnt as bigint) as xcnt,
    xnull, xunique, xblank, xzero, xpos, xneg 
FROM ${tmpdb}.v_conditions 
ORDER BY xtype, xcol;

DROP TABLE covid_conditions;

CREATE VOLATILE TABLE covid_conditions as (
    SELECT * 
    FROM conditions 
    WHERE patient in (SELECT patient from conditions c where c.cond_code = '840539006')
        AND COND_CODE NOT IN ('840539006')
    ) with data
    ON COMMIT PRESERVE ROWS;

select * from covid_conditions cc SAMPLE 5;

call val.td_analyze (
  'frequency',
  'database = ${tmpdb};
   tablename = covid_conditions;
   columns = cond_desc;
   outputdatabase = ${tmpdb};
   outputtablename = cond_frequency;'  
);

select xval, xcnt, xpct from ${tmpdb}.cond_frequency order by xcnt desc;

select xval, xcnt, xpct from ${tmpdb}.cond_frequency where xval like '%(finding)%' order by xcnt desc;

%chart x=xcnt, y=xval, type=bar, width=500, title="Findings in COVID Patients"

DROP TABLE covid_patient_findings;

CREATE VOLATILE TABLE covid_patient_findings AS (
    SELECT PATIENT, ENCOUNTER, REGEXP_REPLACE(oreplace(COND_DESC,' (finding)'), '[^a-zA-Z0-9]+', '') as finding 
    FROM conditions 
    WHERE encounter in (SELECT encounter from conditions c where c.COND_CODE='840539006')
        AND NOT COND_CODE = '840539006'
        AND COND_DESC LIKE '%(finding)%'
    ) WITH DATA
    PRIMARY INDEX (PATIENT)
    ON COMMIT PRESERVE ROWS;


select distinct finding from covid_patient_findings;

DROP TABLE covid_patients;

CREATE VOLATILE MULTISET TABLE covid_patients as (
SELECT DISTINCT
    pat.patient,
    enc.encounter,
    prov.specialty,
    enc.enc_class,
    enc.enc_desc,
    enc.enc_start,
    enc.enc_stop,
    CASE WHEN cast(enc.enc_stop as date) - cast(enc.enc_start as date) = 0 
        THEN 1 
        ELSE cast(enc.enc_stop as date) - cast(enc.enc_start as date) 
    END as enc_days, 
    cast((cast(enc.enc_start as date) -  cast(pat.birthdate as date))/365.25 as integer) as age_at_encounter,
    cast((cast(pat.deathdate as date) -  cast(pat.birthdate as date))/365.25 as integer) as age_at_death,
    CASE WHEN cast(pat.deathdate as date) = cast(enc.enc_start as date) OR cast(pat.deathdate as date) = cast(enc.enc_stop as date)
        THEN 1
        ELSE 0
    END as encounter_death,
    pat.gender, pat.race,
    CASE WHEN con_covid.cond_code = '840539006' THEN 1 ELSE 0 END as COVID,
    enc.payer_coverage,
    enc.total_claim_cost
FROM patients pat 
    INNER JOIN encounters enc on pat.patient=enc.patient
    INNER JOIN providers prov on enc.provider=prov.provider
    INNER JOIN conditions con_covid 
        ON enc.patient=con_covid.patient AND enc.encounter=con_covid.encounter
WHERE con_covid.cond_code = '840539006'
) WITH DATA 
ON COMMIT PRESERVE ROWS;

SELECT patient, covid, enc_days, 
    age_at_encounter, age_at_death, encounter_death, 
    gender, race, payer_coverage, total_claim_cost
FROM covid_patients SAMPLE 10;

CALL val.td_analyze (
    'statistics',
    'database = ${tmpdb};
    tablename = covid_patients;
    columns=allnumeric;
    outputdatabase =${tmpdb};
    outputtablename=s1;
    overwrite=true;
    statisticalmethod=population;');

SELECT xcol, xcnt, xmin, xmax, xmean, xstd  FROM ${tmpdb}.s1;

CALL val.td_analyze (
    'statistics',
    'database = ${tmpdb};
    tablename = covid_patients;
    columns=allnumeric;
    outputdatabase =${tmpdb};
    outputtablename=s2;
    statsoptions=all;
    extendedoptions = all;
    overwrite=true;
    statisticalmethod=population;
    groupby=gender');

SELECT * FROM ${tmpdb}.s2 
--where xcol in ('healthcare_expenses') 
order by xcol, gender;

%chart x=gender, y=xmean, title="Healthcare Expenses by Race", width=300

CALL val.td_analyze (
    'histogram',
    'database = ${tmpdb};
    tablename = covid_patients;
    columns=allnumeric;
    outputdatabase =${tmpdb};
    outputtablename=h1;
    overwrite=true;');

SELECT xcol, xbin, xbeg, xend, xcnt, xpct 
FROM ${tmpdb}.h1 
WHERE xcol = 'age_at_encounter'
ORDER BY xcol, xbin;

drop table findings_ohe_fit;

CREATE VOLATILE TABLE findings_ohe_fit AS (
    SELECT * FROM TD_OneHotEncodingFit (
        ON covid_patient_findings AS InputTable
        USING
            TargetColumn ('finding')
            OtherColumnName ('other')
            CategoricalValues ('Dyspnea','Hemoptysis','Fatigue','Passiveconjunctivalcongestion',
                'Vomitingsymptom','Fever','Cough','Nausea','Nasalcongestion','Diarrheasymptom',
                'Chill','Jointpain','Wheezing','Bodymassindex30obesity','Lossoftaste','Headache',
                'Musclepain','Sputumfinding','Sorethroatsymptom')
            IsInputDense ('true')
  ) AS dt
) WITH DATA 
ON COMMIT PRESERVE ROWS;

drop table findings_ohe;

CREATE VOLATILE TABLE findings_ohe as (
    SELECT * FROM TD_OneHotEncodingTransform (
        ON covid_patient_findings AS InputTable PARTITION BY ANY
        ON findings_ohe_fit AS FitTable DIMENSION
        USING
            IsInputDense ('true')
        ) AS dt
    ) WITH DATA 
    ON COMMIT PRESERVE ROWS;

drop table covid_patients;

CREATE VOLATILE MULTISET TABLE covid_patients as (
SELECT DISTINCT
    pat.patient,
    enc.encounter,
    prov.specialty,
    enc.enc_class,
    enc.enc_desc,
    enc.enc_start,
    enc.enc_stop,
    CASE WHEN cast(enc.enc_stop as date) - cast(enc.enc_start as date) = 0 
        THEN 1 
        ELSE cast(enc.enc_stop as date) - cast(enc.enc_start as date) 
    END as enc_days, 
    cast((cast(enc.enc_start as date) -  cast(pat.birthdate as date))/365.25 as integer) as age_at_encounter,
    cast((cast(pat.deathdate as date) -  cast(pat.birthdate as date))/365.25 as integer) as age_at_death,
    CASE WHEN cast(pat.deathdate as date) = cast(enc.enc_start as date) OR cast(pat.deathdate as date) = cast(enc.enc_stop as date)
        THEN 1
        ELSE 0
    END as encounter_death,
    pat.gender, pat.race,
    CASE WHEN con_covid.cond_code = '840539006' THEN 1 ELSE 0 END as COVID,
    enc.payer_coverage,
    enc.total_claim_cost, 
    cov_f.finding_Dyspnea,cov_f.finding_Hemoptysis,cov_f.finding_Fatigue,cov_f.finding_Passiveconjunctivalcongestion,
    cov_f.finding_Vomitingsymptom,cov_f.finding_Fever,cov_f.finding_Cough,cov_f.finding_Nausea,cov_f.finding_Nasalcongestion,
    cov_f.finding_Diarrheasymptom,cov_f.finding_Chill,cov_f.finding_Jointpain,cov_f.finding_Wheezing,
    cov_f.finding_Bodymassindex30obesity,cov_f.finding_Lossoftaste,cov_f.finding_Headache,cov_f.finding_Musclepain,
    cov_f.finding_Sputumfinding,cov_f.finding_Sorethroatsymptom,cov_f.finding_other
FROM patients pat 
    INNER JOIN encounters enc on pat.patient=enc.patient
    INNER JOIN providers prov on enc.provider=prov.provider
    INNER JOIN conditions con_covid 
        ON enc.patient=con_covid.patient AND enc.encounter=con_covid.encounter
    INNER JOIN ${tmpdb}.findings_ohe cov_f ON enc.patient=cov_f.patient and enc.encounter=cov_f.encounter
WHERE con_covid.cond_code = '840539006'
) WITH DATA 
ON COMMIT PRESERVE ROWS;

select * from ${tmpdb}.covid_patients cov_f sample 3

call val.td_analyze('matrix',
    'database=${tmpdb};
    tablename=covid_patients;
    columns=finding_Dyspnea,finding_Hemoptysis,finding_Fatigue,finding_Passiveconjunctivalcongestion,
        finding_Vomitingsymptom,finding_Fever,finding_Cough,finding_Nausea,finding_Nasalcongestion,finding_Diarrheasymptom,
        finding_Chill,finding_Jointpain,finding_Wheezing,finding_Bodymassindex30obesity,finding_Lossoftaste,finding_Headache,
        finding_Musclepain,finding_Sputumfinding,finding_Sorethroatsymptom,finding_other;
    matrixtype=COR;
    nullhandling=zero;
    outputdatabase = ${tmpdb};
    outputtablename = covid_corr;
    overwrite = true;
    ');

select * from ${tmpdb}.covid_corr order by rownum;

%disconnect Transcend-Production
