#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       06_orders_process_sp/app.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Python Stored Procedures

import time
from snowflake.snowpark import Session, Window
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

TABLE_DICT = {"INCIDENCE_STAGING": "RAW_INCIDENCE_STREAM",
              "MORTALITY_STAGING": "RAW_MORTALITY_STREAM",
              "SURVIVAL_STAGING": "RAW_SURVIVAL_STREAM",
              "TERRITORY_STAGING": "RAW_TERRITORY_STREAM"}

def load_raw_data_stream(session, stage_table, stream):
    stream_df = session.table(f"CANCERANALYTICS_DB.RAW_DATA.{stream}")
    if stream_df.count() != 0:
        print(f"Writing {stream} to {stage_table}")
        stream_df.write.mode("overwrite").save_as_table(f"CANCERANALYTICS_DB.TEMP.{stage_table}")   

def load_all_temp(session):
    session.use_schema("TEMP")

    incidence_df = session.table("CANCERANALYTICS_DB.TEMP.INCIDENCE_STAGING")
    mortality_df = session.table("CANCERANALYTICS_DB.TEMP.MORTALITY_STAGING")
    survival_df = session.table("CANCERANALYTICS_DB.TEMP.SURVIVAL_STAGING")
    incidence_territory_df = session.table("CANCERANALYTICS_DB.TEMP.TERRITORY_STAGING")
    
    return incidence_df, mortality_df, survival_df, incidence_territory_df

def create_data_type_view(session):
    session.use_schema("TEMP")
    incidence_df, mortality_df, survival_df, incidence_territory_df = load_all_temp(session)

    data_type_df = incidence_df.select(F.col("DATA_TYPE")). \
                unionAll(mortality_df.select(F.col("DATA_TYPE"))). \
                unionAll(survival_df.select(F.col("SURVIVAL_TYPE"))). \
                unionAll(incidence_territory_df.select(F.col("DATA_TYPE"))).distinct()
    data_type_df = data_type_df.withColumn("DATA_TYPE_ID", F.md5(F.col("DATA_TYPE"))).sort(F.col("DATA_TYPE"))
    data_type_df = data_type_df.rename(F.col("DATA_TYPE"), "DATA_TYPE_NAME")
    data_type_df = data_type_df.select(F.col("DATA_TYPE_ID"), F.col("DATA_TYPE_NAME"))

    data_type_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.DATA_TYPE_VIEW")

def create_cancer_group_view(session):
    session.use_schema("TEMP")
    incidence_df, mortality_df, survival_df, incidence_territory_df = load_all_temp(session)

    cancer_group_df = incidence_df.select(F.col("CANCER_GROUP_SITE")). \
                    unionAll(mortality_df.select(F.col("CANCER_GROUP_SITE"))). \
                    unionAll(survival_df.select(F.col("CANCER_GROUP_SITE"))). \
                    unionAll(incidence_territory_df.select(F.col("CANCER_GROUP_SITE"))).distinct()
    cancer_group_df = cancer_group_df.withColumn("CANCER_GROUP_ID", F.md5(F.col("CANCER_GROUP_SITE"))).sort(F.col("CANCER_GROUP_SITE"))
    cancer_group_df = cancer_group_df.rename(F.col("CANCER_GROUP_SITE"), "CANCER_GROUP_SITE_NAME")
    cancer_group_df = cancer_group_df.select(F.col("CANCER_GROUP_ID"), F.col("CANCER_GROUP_SITE_NAME"))

    cancer_group_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.CANCER_GROUP_VIEW")

def create_sex_view(session):
    session.use_schema("TEMP")
    incidence_df, mortality_df, survival_df, incidence_territory_df = load_all_temp(session)

    sex_df = (incidence_df.select(F.col("SEX"))
              .unionAll(mortality_df.select(F.col("SEX")))
              .unionAll(survival_df.select(F.col("SEX")))
              .unionAll(incidence_territory_df.select(F.col("SEX")))
              .distinct())
    sex_df = sex_df.withColumn("SEX_ID", F.md5(F.col("SEX"))).sort(F.col("SEX"))
    sex_df = sex_df.rename(F.col("SEX"), "SEX_TYPE")
    sex_df = sex_df.select(F.col("SEX_ID"), F.col("SEX_TYPE"))

    sex_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.SEX_VIEW")

def create_age_group_view(session):
    session.use_schema("TEMP")
    incidence_df, mortality_df, survival_df, _ = load_all_temp(session)

    age_group_df = (incidence_df.select(F.col("AGE_GROUP_YEARS"))
                    .unionAll(mortality_df.select(F.col("AGE_GROUP_YEARS")))
                    .unionAll(survival_df.select(F.col("AGE_GROUP_YEARS")))
                    .distinct())
    age_group_df = age_group_df.withColumn("AGE_GROUP_ID", F.md5(F.col("AGE_GROUP_YEARS"))).sort(F.col("AGE_GROUP_YEARS"))
    age_group_df = age_group_df.select(F.col("AGE_GROUP_ID"), F.col("AGE_GROUP_YEARS"))

    age_group_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.AGE_GROUP_VIEW")

def create_state_territory_view(session):
    session.use_schema("TEMP")
    _, _, _, incidence_territory_df = load_all_temp(session)

    state_territory_df = incidence_territory_df.select(F.col("STATE_OR_TERRITORY")).distinct()
    state_territory_df = state_territory_df.withColumn("STATE_TERRITORY_ID", F.md5(F.col("STATE_OR_TERRITORY"))).sort(F.col("STATE_OR_TERRITORY"))
    state_territory_df = state_territory_df.rename(F.col("STATE_OR_TERRITORY"), "STATE_OR_TERRITORY_NAME")
    state_territory_df = state_territory_df.select(F.col("STATE_TERRITORY_ID"), F.col("STATE_OR_TERRITORY_NAME"))
    
    state_territory_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.STATE_TERRITORY_VIEW")

def create_dimension_views(session):
    create_data_type_view(session)
    create_cancer_group_view(session)
    create_sex_view(session)
    create_age_group_view(session)
    create_state_territory_view(session)

def load_all_dimension_views(session):
    session.use_schema("TRANSFORMED_DATA")

    data_type_dim = session.table("CANCERANALYTICS_DB.TEMP.DATA_TYPE_VIEW")
    cancer_group_dim = session.table("CANCERANALYTICS_DB.TEMP.CANCER_GROUP_VIEW")
    sex_dim = session.table("CANCERANALYTICS_DB.TEMP.SEX_VIEW")
    age_group_dim = session.table("CANCERANALYTICS_DB.TEMP.AGE_GROUP_VIEW")
    state_territory_dim = session.table("CANCERANALYTICS_DB.TEMP.STATE_TERRITORY_VIEW")

    return data_type_dim, cancer_group_dim, sex_dim, age_group_dim, state_territory_dim
    
def create_incidence_view(session):
    session.use_schema("TEMP")
    incidence_raw = session.table("CANCERANALYTICS_DB.TEMP.INCIDENCE_STAGING")
    data_type_dim, cancer_group_dim, sex_dim, age_group_dim, _ = load_all_dimension_views(session)

    incidence_df = incidence_raw.join(data_type_dim, incidence_raw['DATA_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                                .join(cancer_group_dim, incidence_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                                .join(sex_dim, incidence_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                                .join(age_group_dim, incidence_raw["AGE_GROUP_YEARS"] == age_group_dim["AGE_GROUP_YEARS"], rsuffix='_r') \
                                .select(F.md5(F.concat_ws(F.col("DATA_TYPE_ID"),F.col("CANCER_GROUP_ID"), F.col("YEAR"), F.col("SEX_ID"), F.col("AGE_GROUP_ID"), F.col("COUNT"))).alias("INCIDENCE_ID"),
                                        F.col("DATA_TYPE_ID"),
                                        F.col("CANCER_GROUP_ID"), 
                                        F.col("YEAR"), 
                                        F.col("SEX_ID"), 
                                        F.col("AGE_GROUP_ID"), 
                                        F.col("COUNT"))
    
    incidence_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.INCIDENCE_VIEW")

def create_mortality_view(session):
    session.use_schema("TEMP")
    mortality_raw = session.table("CANCERANALYTICS_DB.TEMP.MORTALITY_STAGING")
    data_type_dim, cancer_group_dim, sex_dim, age_group_dim, _ = load_all_dimension_views(session)

    mortality_df = mortality_raw.join(data_type_dim, mortality_raw['DATA_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                                .join(cancer_group_dim, mortality_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                                .join(sex_dim, mortality_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                                .join(age_group_dim, mortality_raw["AGE_GROUP_YEARS"] == age_group_dim["AGE_GROUP_YEARS"]) \
                                .select(F.md5(F.concat_ws(F.col("DATA_TYPE_ID"),F.col("CANCER_GROUP_ID"), F.col("YEAR"), F.col("SEX_ID"), F.col("AGE_GROUP_ID"), F.col("COUNT"))).alias("MORTALITY_ID"),
                                        F.col("DATA_TYPE_ID"),
                                        F.col("CANCER_GROUP_ID"), 
                                        F.col("YEAR"), 
                                        F.col("SEX_ID"), 
                                        F.col("AGE_GROUP_ID"), 
                                        F.col("COUNT"))
    
    mortality_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.MORTALITY_VIEW")

def create_survival_view(session):
    session.use_schema("TEMP")
    survival_raw = session.table("CANCERANALYTICS_DB.TEMP.SURVIVAL_STAGING")

    data_type_dim, cancer_group_dim, sex_dim, age_group_dim, _ = load_all_dimension_views(session)
    survival_df = survival_raw.join(data_type_dim, survival_raw['SURVIVAL_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                            .join(cancer_group_dim, survival_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                            .join(sex_dim, survival_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                            .join(age_group_dim, survival_raw["AGE_GROUP_YEARS"] == age_group_dim["AGE_GROUP_YEARS"]) \
                            .select(F.md5(F.concat_ws(F.col("DATA_TYPE_ID"),F.col("CANCER_GROUP_ID"), F.col("PERIOD_YEARS"), F.col("SEX_ID"), F.col("YEARS_AFTER_DIAGNOSIS"), F.col("AGE_GROUP_ID"), F.col("SURVIVAL"), F.col("95_CI_LOWER_BOUND"), F.col("95_CI_UPPER_BOUND"))).alias("SURVIVAL_ID"),
                                    F.col("DATA_TYPE_ID"),
                                    F.col("CANCER_GROUP_ID"),
                                    F.col("PERIOD_YEARS"), 
                                    F.col("SEX_ID"), 
                                    F.col("YEARS_AFTER_DIAGNOSIS"),
                                    F.col("AGE_GROUP_ID"), 
                                    F.col("SURVIVAL"),
                                    F.col("95_CI_LOWER_BOUND"),
                                    F.col("95_CI_UPPER_BOUND"))
    
    survival_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.SURVIVAL_VIEW")

def create_territory_view(session):
    session.use_schema("TEMP")
    incidence_territory_raw = session.table("CANCERANALYTICS_DB.TEMP.TERRITORY_STAGING")

    data_type_dim, cancer_group_dim, sex_dim, _, state_territory_dim = load_all_dimension_views(session)
    incidence_territory_df = incidence_territory_raw.join(data_type_dim, incidence_territory_raw['DATA_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                                                    .join(cancer_group_dim, incidence_territory_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                                                    .join(sex_dim, incidence_territory_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                                                    .join(state_territory_dim, incidence_territory_raw["STATE_OR_TERRITORY"] == state_territory_dim["STATE_OR_TERRITORY_NAME"]) \
                                                    .select(F.md5(F.concat_ws(F.col("DATA_TYPE_ID"),F.col("CANCER_GROUP_ID"), F.col("YEAR"), F.col("SEX_ID"), F.col("STATE_TERRITORY_ID"), F.col("COUNT"))).alias("INCIDENCE_TERRITORY_ID"),
                                                            F.col("DATA_TYPE_ID"),
                                                            F.col("CANCER_GROUP_ID"), 
                                                            F.col("YEAR"), 
                                                            F.col("SEX_ID"), 
                                                            F.col("STATE_TERRITORY_ID"), 
                                                            F.col("COUNT"))
    
    incidence_territory_df.create_or_replace_view("CANCERANALYTICS_DB.TEMP.INCIDENCE_TERRITORY_VIEW")

def create_fact_views(session):
    create_incidence_view(session)
    create_mortality_view(session)
    create_survival_view(session)
    create_territory_view(session)

def test_views(session):
    session.use_schema('TEMP')
    data_type_dim = session.table("CANCERANALYTICS_DB.TEMP.DATA_TYPE_VIEW")
    cancer_group_dim = session.table("CANCERANALYTICS_DB.TEMP.CANCER_GROUP_VIEW")
    sex_dim = session.table("CANCERANALYTICS_DB.TEMP.SEX_VIEW")
    age_group_dim = session.table("CANCERANALYTICS_DB.TEMP.AGE_GROUP_VIEW")
    state_territory_dim = session.table("CANCERANALYTICS_DB.TEMP.STATE_TERRITORY_VIEW")

    incidence_fact = session.table("CANCERANALYTICS_DB.TEMP.INCIDENCE_VIEW")
    mortality_fact = session.table("CANCERANALYTICS_DB.TEMP.MORTALITY_VIEW")
    survival_fact = session.table("CANCERANALYTICS_DB.TEMP.SURVIVAL_VIEW")
    territory_fact = session.table("CANCERANALYTICS_DB.TEMP.INCIDENCE_TERRITORY_VIEW")

    data_type_dim.limit(5).show()   
    cancer_group_dim.limit(5).show()  
    sex_dim.limit(5).show()  
    age_group_dim.limit(5).show()
    state_territory_dim.limit(5).show()

    incidence_fact.limit(5).show()
    mortality_fact.limit(5).show()
    survival_fact.limit(5).show()
    territory_fact.limit(5).show()

def main(session: Session) -> str:
    # Create the ORDERS table and ORDERS_STREAM stream if they don't exist
    for stage_table_name, stream_name in TABLE_DICT.items():
        load_raw_data_stream(session, stage_table_name, stream_name)

    create_dimension_views(session)
    create_incidence_view(session)
    create_fact_views(session)
    test_views(session)

    return f"Successfully transformed"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils, helper
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
