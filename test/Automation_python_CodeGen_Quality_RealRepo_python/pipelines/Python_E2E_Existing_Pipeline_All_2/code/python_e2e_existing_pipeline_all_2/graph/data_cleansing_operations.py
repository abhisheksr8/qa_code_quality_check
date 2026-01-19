from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def data_cleansing_operations(spark: SparkSession, df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap
    from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, ShortType
    # Remove rows where all columns are null
    df = df.na.drop(how = "all")
    # Step 2: Apply data cleansing operations
    # Start with the original columns
    transformed_columns = []

    # Check if column exists after null operations
    if "customer_id_final" not in df.columns:
        print(
            "Warning: Column 'customer_id_final' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["customer_id_final"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["customer_id_final"].dataType, StringType):
            df = df.na.fill({"customer_id_final" : "NA"})
            transformed_columns = [initcap(
                                       upper(
                                         lower(
                                           regexp_replace(
                                             regexp_replace(
                                               regexp_replace(
                                                 regexp_replace(
                                                   regexp_replace(trim(col("customer_id_final")), r'\s+', ' '),
                                                   r'\s+',
                                                   ''
                                                 ),
                                                 r'[A-Za-z]',
                                                 ''
                                               ),
                                               r'[^\w\s]',
                                               ''
                                             ),
                                             r'\d+',
                                             ''
                                           )
                                         )
                                       )
                                     )\
                                     .alias("customer_id_final")]
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"customer_id_final" : 0})
            transformed_columns = [col("customer_id_final")]
        else:
            transformed_columns = [col("customer_id_final")]

    # Check if column exists after null operations
    if "account_flags" not in df.columns:
        print(
            "Warning: Column 'account_flags' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["account_flags"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["account_flags"].dataType, StringType):
            df = df.na.fill({"account_flags" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("account_flags")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("account_flags")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"account_flags" : 0})
            transformed_columns.append(col("account_flags"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("account_flags"))

    # Check if column exists after null operations
    if "account_open_date" not in df.columns:
        print(
            "Warning: Column 'account_open_date' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["account_open_date"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["account_open_date"].dataType, StringType):
            df = df.na.fill({"account_open_date" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("account_open_date")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("account_open_date")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"account_open_date" : 0})
            transformed_columns.append(col("account_open_date"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("account_open_date"))

    # Check if column exists after null operations
    if "country_code" not in df.columns:
        print(
            "Warning: Column 'country_code' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["country_code"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["country_code"].dataType, StringType):
            df = df.na.fill({"country_code" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("country_code")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("country_code")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"country_code" : 0})
            transformed_columns.append(col("country_code"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("country_code"))

    # Check if column exists after null operations
    if "first_name" not in df.columns:
        print(
            "Warning: Column 'first_name' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["first_name"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["first_name"].dataType, StringType):
            df = df.na.fill({"first_name" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("first_name")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("first_name")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"first_name" : 0})
            transformed_columns.append(col("first_name"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("first_name"))

    # Check if column exists after null operations
    if "last_name" not in df.columns:
        print("Warning: Column 'last_name' not found after null operation. Skipping transformations for this column.")
    else:
        col_type = df.schema["last_name"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["last_name"].dataType, StringType):
            df = df.na.fill({"last_name" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("last_name")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("last_name")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"last_name" : 0})
            transformed_columns.append(col("last_name"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("last_name"))

    # Check if column exists after null operations
    if "new_email" not in df.columns:
        print("Warning: Column 'new_email' not found after null operation. Skipping transformations for this column.")
    else:
        col_type = df.schema["new_email"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["new_email"].dataType, StringType):
            df = df.na.fill({"new_email" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("new_email")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("new_email")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"new_email" : 0})
            transformed_columns.append(col("new_email"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("new_email"))

    # Check if column exists after null operations
    if "new_phone" not in df.columns:
        print("Warning: Column 'new_phone' not found after null operation. Skipping transformations for this column.")
    else:
        col_type = df.schema["new_phone"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["new_phone"].dataType, StringType):
            df = df.na.fill({"new_phone" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("new_phone")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("new_phone")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"new_phone" : 0})
            transformed_columns.append(col("new_phone"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("new_phone"))

    # Check if column exists after null operations
    if "customer_id" not in df.columns:
        print(
            "Warning: Column 'customer_id' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["customer_id"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["customer_id"].dataType, StringType):
            df = df.na.fill({"customer_id" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(
                initcap(
                    upper(
                      lower(
                        regexp_replace(
                          regexp_replace(
                            regexp_replace(
                              regexp_replace(regexp_replace(trim(col("customer_id")), r'\s+', ' '), r'\s+', ''),
                              r'[A-Za-z]',
                              ''
                            ),
                            r'[^\w\s]',
                            ''
                          ),
                          r'\d+',
                          ''
                        )
                      )
                    )
                  )\
                  .alias("customer_id")
            )
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            df = df.na.fill({"customer_id" : 0})
            transformed_columns.append(col("customer_id"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("customer_id"))

    df = df.select(
        *[
          col(c)
          for c in df.columns
          if (
          c
          not in ["customer_id_final",  "account_flags",  "account_open_date",  "country_code",  "first_name",  "last_name",  "new_email",              "new_phone",  "customer_id"]
        )
        ],
        *transformed_columns
    )

    return df
