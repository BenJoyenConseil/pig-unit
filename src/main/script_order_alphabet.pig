
data = LOAD 'input' AS (name:CHARARRAY);

data_ordered = ORDER data BY name;
