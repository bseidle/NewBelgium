-- Recursive CTE to iterate over hierarchy top to bottom to align sales managers 
-- Can be used to preformat results for visualization tools like PBI when performance is limited or to troubleshoot misaligned hierarchies if modified



WITH SalesManager (ParentID, EmployeeID, SalesManager, SalesRegion)
AS (SELECT ParentID,
           EmployeeID,
           SalesManager,
           SalesRegion
    FROM dbo.SalesManagerRegion
    WHERE ParentID = 0 -- grab the root of the hierarchy 
    UNION ALL
    SELECT h.ParentID,
           h.EmployeeID,
           h.SalesManager,
           h.SalesRegion
    FROM dbo.SalesManagerRegion AS h
        INNER JOIN SalesManager AS e
            ON h.ParentID = e.EmployeeID -- iterate down from the parent root
   )
SELECT ParentID,
       EmployeeID,
       SalesManager,
       SalesRegion
FROM SalesManager
ORDER BY ParentID;