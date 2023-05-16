# Datlo Test

Vídeo explicativo: https://youtu.be/u4wdHA8jMQQ

Total execution time de 29 min

| Task               | Time                                                |
| ----------------- | ---------------------------------------------------------------- |
| Lambda Execution       | 9 min |
| Creation Cluster EMR| 7 min |
| Pyspark Script EMR    | 13 min| 
|Total|29 min


1. Lambda Execution - 9 min in which:
  -  3 minutes to initialize EC2 instance
  -  6 minutes to download, extract and make the data available in a S3 Bucket

2. Creation Cluster EMR - 7 minutos

3. Script EMR - 13 minutos
- Saving in csv and parquet formats


