SELECT
    mloc.zip_code,
    mloc.state_code,
    meta.state_name,
    meta.fee_schedule_area,
    gpci.locality_name,
    meta.counties,
    gpci.locality_code,
    gpci.work_gpci,
    gpci.pe_gpci,
    gpci.mp_gpci,
    rvu.procedure_code,
    rvu.modifier,
    rvu.work_rvu,
    rvu.practice_expense_rvu,
    rvu.malpractice_rvu,
    rvu.total_rvu,
    cf.conversion_factor,
    -- The actual Medicare allowed amount calculation:
    (
      (
        COALESCE(rvu.work_rvu, 0) * COALESCE(gpci.work_gpci, 0) +
        COALESCE(rvu.practice_expense_rvu, 0) * COALESCE(gpci.pe_gpci, 0) +
        COALESCE(rvu.malpractice_rvu, 0) * COALESCE(gpci.mp_gpci, 0)
      ) * COALESCE(cf.conversion_factor, 0)
    ) AS allowed_amount
FROM
    medicare_locality_map mloc
JOIN
    medicare_locality_meta meta
    ON mloc.carrier_code = meta.mac_code
    AND mloc.locality_code = meta.locality_code
JOIN
    cms_gpci gpci
    ON TRIM(meta.fee_schedule_area) = TRIM(gpci.locality_name)
    AND mloc.locality_code = gpci.locality_code
JOIN
    cms_rvu rvu
    -- You’ll filter on procedure_code and modifier in WHERE
    ON 1=1
JOIN
    cms_conversion_factor cf
    ON gpci.year = cf.year
WHERE
    mloc.zip_code = '15044'
    AND gpci.year = 2025
    AND rvu.year = 2025
    AND rvu.procedure_code = '73721'
    AND (rvu.modifier IS NULL OR rvu.modifier = '')
;