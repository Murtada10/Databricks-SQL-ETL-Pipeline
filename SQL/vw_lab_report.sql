-- Presentation Layer View for Power BI Executive Reporting
CREATE OR ALTER VIEW dbo.vw_Lab_Report AS
SELECT
    CandidateNID         AS [National ID],
    InvestigationID      AS [Investigation ID],
    InvestigationCode    AS [Investigation Code],
    InvestigationDate    AS [Investigation Date],
    RequisitionCode      AS [Requisition Code],
    PhysicianRequestDate AS [Physician Request Date],
    SampleID             AS [Sample ID],
    CASE
        WHEN UPPER(LTRIM(RTRIM(TestStatus))) IN ('FINAL', 'COMPLETE', 'SUBMITTED', 'REPORTED')
            THEN 'received'
        ELSE 'not received'
    END                  AS [Lab Status],
    TestName             AS [Test Name],
    Disease              AS [Disease]
FROM dbo.Staging_Lab_Requisitions;
GO

-- UAT Validation Query (run manually after ETL load)
-- SELECT TOP (100)
--     [National ID],
--     [Investigation ID],
--     [Investigation Code],
--     [Requisition Code],
--     [Physician Request Date],
--     [Sample ID],
--     [Lab Status],
--     [Test Name],
--     [Disease]
-- FROM dbo.vw_Lab_Report;
