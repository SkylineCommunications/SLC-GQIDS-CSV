# SLC-GQIDS-CSV

A custom GQI data source to import comma-separated values (CSV) from a file.

## Type conversion

The columns can be automatically parsed to a specific type by suffixing the column name in the CSV header with `::type`.
Where `type` can be one of the following:

- `bool`
- `datetime`
- `double`
- `int`
- `string` (default)

### Example

```CSV
Timestamp::datetime,Test name,Test cases::int,Duration::double,Success::boolean
06/12/2023 01:00,Cisco CMTS,36,1081.788,false
06/12/2023 01:21,Huawei 5600 5800,4,196.621,true
06/12/2023 01:26,Cisco CBR8,41,1443.027,true
06/12/2023 01:50,Arris E6000,33,989.310,false
06/12/2023 02:08,Casa Systems,12,374.005,true
```
