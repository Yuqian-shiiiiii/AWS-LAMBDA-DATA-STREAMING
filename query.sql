SELECT t.name,
        t.hour,
        t.highest,
        i.ts
FROM 
    (SELECT name,
         max(high) AS highest,
         hour
    FROM 
        (SELECT name,
         high,
         ts,
         substring(ts,
         12,
         2) AS hour
        FROM s3_project3)
        GROUP BY  (name, hour)) AS t
    INNER JOIN 
    (SELECT name,
        high,
        ts,
        substr(ts,
        12,
        2)as hour
    FROM s3_project3) AS i
    ON t.highest=i.high
        AND t.hour=i.hour
        AND t.name=i.name
ORDER BY  (name,hour)
