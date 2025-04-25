
  
    
    
    
        
        insert into default.country__dbt_backup
        ("id", "name", "age", "created_at")SELECT *
FROM s3('http://minio:9002/datalake/MTSZN/WMARKET/current/labor.parquet/city=*/*.parquet',
        'minioadmin',
        'minioadmin',
        'Parquet')
  
  