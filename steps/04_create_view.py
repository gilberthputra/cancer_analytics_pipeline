from snowflake.snowpark import Session, Window
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def load_all_raw_data_streams(session):
    session.use_schema("RAW_DATA")

    incidence_df = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_INCIDENCE_STREAM")
    mortality_df = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_MORTALITY_STREAM")
    survival_df = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_SURVIVAL_STREAM")
    incidence_territory_df = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_TERRITORY_STREAM")
    
    return incidence_df, mortality_df, survival_df, incidence_territory_df

def create_data_type_view(session):
    incidence_df, mortality_df, survival_df, incidence_territory_df = load_all_raw_data_streams(session)

    session.use_schema("TRANSFORMED_DATA")
    data_type_df = incidence_df.select(F.col("DATA_TYPE")). \
                unionAll(mortality_df.select(F.col("DATA_TYPE"))). \
                unionAll(survival_df.select(F.col("SURVIVAL_TYPE"))). \
                unionAll(incidence_territory_df.select(F.col("DATA_TYPE"))).distinct()
    data_type_df = data_type_df.withColumn("DATA_TYPE_ID", F.row_number().over(Window.orderBy("DATA_TYPE")))
    data_type_df = data_type_df.rename(F.col("DATA_TYPE"), "DATA_TYPE_NAME")
    data_type_df = data_type_df.select(F.col("DATA_TYPE_ID"), F.col("DATA_TYPE_NAME"))
    data_type_df = data_type_df.select(F.md5(F.col("DATA_TYPE")).alias("DATA_TYPE_ID"), 
                                       F.col("DATA_TYPE").alias("DATA_TYPE_NAME"))

    data_type_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.DATA_TYPE_VIEW")

def create_cancer_group_view(session):
    incidence_df, mortality_df, survival_df, incidence_territory_df = load_all_raw_data_streams(session)

    session.use_schema("TRANSFORMED_DATA")

    cancer_group_df = incidence_df.select(F.col("CANCER_GROUP_SITE")). \
                    unionAll(mortality_df.select(F.col("CANCER_GROUP_SITE"))). \
                    unionAll(survival_df.select(F.col("CANCER_GROUP_SITE"))). \
                    unionAll(incidence_territory_df.select(F.col("CANCER_GROUP_SITE"))).distinct()
    cancer_group_df = cancer_group_df.withColumn("CANCER_GROUP_ID", F.row_number().over(Window.orderBy("CANCER_GROUP_SITE")))
    cancer_group_df = cancer_group_df.rename(F.col("CANCER_GROUP_SITE"), "CANCER_GROUP_SITE_NAME")
    cancer_group_df = cancer_group_df.select(F.col("CANCER_GROUP_ID"), F.col("CANCER_GROUP_SITE_NAME"))

    cancer_group_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.CANCER_GROUP_VIEW")

def create_sex_view(session):
    incidence_df, mortality_df, survival_df, incidence_territory_df = load_all_raw_data_streams(session)

    session.use_schema("TRANSFORMED_DATA")

    sex_df = (incidence_df.select(F.col("SEX"))
              .unionAll(mortality_df.select(F.col("SEX")))
              .unionAll(survival_df.select(F.col("SEX")))
              .unionAll(incidence_territory_df.select(F.col("SEX")))
              .distinct())
    sex_df = sex_df.withColumn("SEX_ID", F.row_number().over(Window.orderBy("SEX")))
    sex_df = sex_df.rename(F.col("SEX"), "SEX_TYPE")
    sex_df = sex_df.select(F.col("SEX_ID"), F.col("SEX_TYPE"))

    sex_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.SEX_VIEW")

def create_age_group_view(session):
    incidence_df, mortality_df, survival_df, _ = load_all_raw_data_streams(session)

    session.use_schema("TRANSFORMED_DATA")
    age_group_df = (incidence_df.select(F.col("AGE_GROUP_YEARS"))
                    .unionAll(mortality_df.select(F.col("AGE_GROUP_YEARS")))
                    .unionAll(survival_df.select(F.col("AGE_GROUP_YEARS")))
                    .distinct())
    age_group_df = age_group_df.withColumn("AGE_GROUP_ID", F.row_number().over(Window.orderBy("AGE_GROUP_YEARS")))
    age_group_df = age_group_df.select(F.col("AGE_GROUP_ID"), F.col("AGE_GROUP_YEARS"))

    age_group_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.AGE_GROUP_VIEW")

def create_state_territory_view(session):
    _, _, _, incidence_territory_df = load_all_raw_data_streams(session)

    session.use_schema("TRANSFORMED_DATA")

    state_territory_df = incidence_territory_df.select(F.col("STATE_OR_TERRITORY")).distinct()
    state_territory_df = state_territory_df.withColumn("STATE_TERRITORY_ID", F.row_number().over(Window.orderBy("STATE_OR_TERRITORY")))
    state_territory_df = state_territory_df.rename(F.col("STATE_OR_TERRITORY"), "STATE_OR_TERRITORY_NAME")
    state_territory_df = state_territory_df.select(F.col("STATE_TERRITORY_ID"), F.col("STATE_OR_TERRITORY_NAME"))
    
    state_territory_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.STATE_TERRITORY_VIEW")

def create_dimension_views(session):
    create_data_type_view(session)
    create_cancer_group_view(session)
    create_sex_view(session)
    create_age_group_view(session)
    create_state_territory_view(session)

def load_all_dimension_views(session):
    session.use_schema("TRANSFORMED_DATA")

    data_type_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.DATA_TYPE_VIEW")
    cancer_group_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.CANCER_GROUP_VIEW")
    sex_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.SEX_VIEW")
    age_group_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.AGE_GROUP_VIEW")
    state_territory_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.STATE_TERRITORY_VIEW")

    return data_type_dim, cancer_group_dim, sex_dim, age_group_dim, state_territory_dim
    
def create_incidence_view(session):
    session.use_schema("RAW_DATA")
    incidence_raw = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_INCIDENCE_STREAM")

    session.use_schema("TRANSFORMED_DATA")
    data_type_dim, cancer_group_dim, sex_dim, age_group_dim, _ = load_all_dimension_views(session)

    incidence_df = incidence_raw.join(data_type_dim, incidence_raw['DATA_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                                .join(cancer_group_dim, incidence_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                                .join(sex_dim, incidence_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                                .join(age_group_dim, incidence_raw["AGE_GROUP_YEARS"] == age_group_dim["AGE_GROUP_YEARS"], rsuffix='_r') \
                                .select(F.row_number().over(Window.orderBy("YEAR")).alias("INCIDENCE_ID"),
                                        F.col("DATA_TYPE_ID"),
                                        F.col("CANCER_GROUP_ID"), 
                                        F.col("YEAR"), 
                                        F.col("SEX_ID"), 
                                        F.col("AGE_GROUP_ID"), 
                                        F.col("COUNT"))
    
    incidence_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE_VIEW")

def create_mortality_view(session):
    session.use_schema("RAW_DATA")
    mortality_raw = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_MORTALITY_STREAM")

    session.use_schema("TRANSFORMED_DATA")
    data_type_dim, cancer_group_dim, sex_dim, age_group_dim, _ = load_all_dimension_views(session)

    mortality_df = mortality_raw.join(data_type_dim, mortality_raw['DATA_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                                .join(cancer_group_dim, mortality_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                                .join(sex_dim, mortality_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                                .join(age_group_dim, mortality_raw["AGE_GROUP_YEARS"] == age_group_dim["AGE_GROUP_YEARS"]) \
                                .select(F.row_number().over(Window.orderBy("YEAR")).alias("INCIDENCE_ID"),
                                        F.col("DATA_TYPE_ID"),
                                        F.col("CANCER_GROUP_ID"), 
                                        F.col("YEAR"), 
                                        F.col("SEX_ID"), 
                                        F.col("AGE_GROUP_ID"), 
                                        F.col("COUNT"))
    
    mortality_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.MORTALITY_VIEW")

def create_survival_view(session):
    session.use_schema("RAW_DATA")
    survival_raw = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_SURVIVAL_STREAM")

    session.use_schema("TRANSFORMED_DATA")
    data_type_dim, cancer_group_dim, sex_dim, age_group_dim, _ = load_all_dimension_views(session)
    survival_df = survival_raw.join(data_type_dim, survival_raw['SURVIVAL_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                            .join(cancer_group_dim, survival_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                            .join(sex_dim, survival_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                            .join(age_group_dim, survival_raw["AGE_GROUP_YEARS"] == age_group_dim["AGE_GROUP_YEARS"]) \
                            .select(F.row_number().over(Window.orderBy("CANCER_GROUP_SITE", "SURVIVAL_TYPE")).alias("SURVIVAL_ID"),
                                    F.col("DATA_TYPE_ID"),
                                    F.col("CANCER_GROUP_ID"),
                                    F.col("PERIOD_YEARS"), 
                                    F.col("SEX_ID"), 
                                    F.col("YEARS_AFTER_DIAGNOSIS"),
                                    F.col("AGE_GROUP_ID"), 
                                    F.col("SURVIVAL"),
                                    F.col("95_CI_LOWER_BOUND"),
                                    F.col("95_CI_UPPER_BOUND"))
    survival_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.SURVIVAL_VIEW")

def create_territory_view(session):
    session.use_schema("RAW_DATA")
    incidence_territory_raw = session.table("CANCERANALYTICS_DB.RAW_DATA.RAW_TERRITORY_STREAM")

    session.use_schema("TRANSFORMED_DATA")
    data_type_dim, cancer_group_dim, sex_dim, _, state_territory_dim = load_all_dimension_views(session)

    incidence_territory_df = incidence_territory_raw.join(data_type_dim, incidence_territory_raw['DATA_TYPE'] == data_type_dim['DATA_TYPE_NAME']) \
                                                    .join(cancer_group_dim, incidence_territory_raw["CANCER_GROUP_SITE"] == cancer_group_dim["CANCER_GROUP_SITE_NAME"]) \
                                                    .join(sex_dim, incidence_territory_raw["SEX"] == sex_dim["SEX_TYPE"]) \
                                                    .join(state_territory_dim, incidence_territory_raw["STATE_OR_TERRITORY"] == state_territory_dim["STATE_OR_TERRITORY_NAME"]) \
                                                    .select(F.row_number().over(Window.orderBy("YEAR")).alias("INCIDENCE_ID"),
                                                            F.col("DATA_TYPE_ID"),
                                                            F.col("CANCER_GROUP_ID"), 
                                                            F.col("YEAR"), 
                                                            F.col("SEX_ID"), 
                                                            F.col("STATE_TERRITORY_ID"), 
                                                            F.col("COUNT"))
    
    incidence_territory_df.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE_TERRITORY_VIEW")

def create_fact_views(session):
    create_incidence_view(session)
    create_mortality_view(session)
    create_survival_view(session)
    create_territory_view(session)

def test_views(session):
    session.use_schema('TRANSFORMED_DATA')
    data_type_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.DATA_TYPE_VIEW")
    cancer_group_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.CANCER_GROUP_VIEW")
    sex_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.SEX_VIEW")
    age_group_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.AGE_GROUP_VIEW")
    state_territory_dim = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.STATE_TERRITORY_VIEW")

    incidence_fact = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE_VIEW")
    mortality_fact = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.MORTALITY_VIEW")
    survival_fact = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.SURVIVAL_VIEW")
    territory_fact = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE_TERRITORY_VIEW")

    data_type_dim.limit(5).show()   
    cancer_group_dim.limit(5).show()  
    sex_dim.limit(5).show()  
    age_group_dim.limit(5).show()
    state_territory_dim.limit(5).show()

    incidence_fact.limit(5).show()
    mortality_fact.limit(5).show()
    survival_fact.limit(5).show()
    territory_fact.limit(5).show()

# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    incidence_df, mortality_df, survival_df, incidence_territory_df = load_all_raw_data_streams(session)

    incidence_df.show()

    create_dimension_views(session)
    # create_fact_views(session)
    # test_views(session)

    session.close()