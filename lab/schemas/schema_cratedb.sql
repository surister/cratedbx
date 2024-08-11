CREATE TABLE IF NOT EXISTS "doc"."simple_array" (
   "id" TEXT,
   "txt" TEXT,
   "ip" IP,
   "i16" SMALLINT,
   "u16" SMALLINT,
   "i32" INTEGER,
   "u32" INTEGER,
   "i32_from_text" INTEGER,
   "f32" REAL,
   "f64" DOUBLE PRECISION,
   "bool" BOOLEAN,
   "null_" TEXT,
   "array_i32" ARRAY(INTEGER),
   "array_u32" ARRAY(INTEGER),
   "array_text" ARRAY(TEXT),
   "array_mixed" ARRAY(TEXT),
   "empty_array" ARRAY(TEXT),
   "empty_multi_arrays" ARRAY(ARRAY(TEXT)),
   "datetime" TIMESTAMP WITHOUT TIME ZONE,
   "datetime_2" TIMESTAMP WITH TIME ZONE,
   "obj" OBJECT,
   "vector_float" FLOAT_VECTOR(2048),
   "vector_float_simple" FLOAT_VECTOR(1)
)