WITH expected_tests AS (
        SELECT * FROM (
            VALUES 
                -- Hematology / CBC Panel
                ('HAEMOGLOBIN'), ('PCV'), ('RBC COUNT'), ('MCV'), ('MCH'), ('MCHC'), ('R.D.W'),
                ('TOTAL LEUCOCYTE COUNT (TLC)'), ('NEUTROPHILS'), ('LYMPHOCYTES'), 
                ('EOSINOPHILS'), ('MONOCYTES'), ('BASOPHILS'), ('ABSOLUTE NEUTROPHILS'), 
                ('ABSOLUTE LYMPHOCYTES'), ('ABSOLUTE EOSINOPHILS'), ('ABSOLUTE MONOCYTES'), 
                ('ABSOLUTE BASOPHILS'), ('PLATELET COUNT'), ('MPV'),
                -- Biochemistry / Liver Function Test (LFT) Panel
                ('BILIRUBIN, TOTAL'), ('BILIRUBIN CONJUGATED (DIRECT)'), ('BILIRUBIN (INDIRECT)'), 
                ('ALANINE AMINOTRANSFERASE (ALT/SGPT)'), ('ASPARTATE AMINOTRANSFERASE (AST/SGOT)'), 
                ('AST (SGOT) / ALT (SGPT) RATIO (DE RITIS)'), ('ALKALINE PHOSPHATASE'), 
                ('PROTEIN, TOTAL'), ('ALBUMIN'), ('GLOBULIN'), ('A/G RATIO')
        ) AS t (expected_test_name)
    ),
    actual_observations AS (
        SELECT UPPER(TRIM(test_name)) AS actual_test_name, numeric_value, units, interpretation
        FROM iceberg.clinical_analytics.lab_observations
        WHERE report_id = 'LAB-DMRROPV10596'
    )
    SELECT 
        e.expected_test_name,
        CASE 
            WHEN a.actual_test_name IS NOT NULL THEN 'MATCH'
            ELSE 'MISSING FROM INGESTION'
        END AS ingestion_status,
        a.numeric_value AS captured_value,
        a.units AS captured_units,
        a.interpretation
    FROM expected_tests e
    LEFT JOIN actual_observations a 
        ON e.expected_test_name = a.actual_test_name
    ORDER BY ingestion_status DESC, e.expected_test_name ASC;