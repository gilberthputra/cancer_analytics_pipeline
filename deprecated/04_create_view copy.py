from snowflake.snowpark import Session, Window
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def create_dimension_views(session, raw_schema='', transformed_schema=''):
    session.use_schema(raw_schema)

    incidence_df = session.table("CANCERANALYTICS_DB.RAW_DATA.INCIDENCE")
    mortality_df = session.table("CANCERANALYTICS_DB.RAW_DATA.MORTALITY")
    survival_df = session.table("CANCERANALYTICS_DB.RAW_DATA.SURVIVAL")
    incidence_territory_df = session.table("CANCERANALYTICS_DB.RAW_DATA.INCIDENCE_TERRITORY")

    session.use_schema(transformed_schema)

    data_type_df = incidence_df.select(F.col("DATA_TYPE")). \
                unionAll(mortality_df.select(F.col("DATA_TYPE"))). \
                unionAll(survival_df.select(F.col("SURVIVAL_TYPE"))). \
                unionAll(incidence_territory_df.select(F.col("DATA_TYPE"))).distinct()
    data_type_df = data_type_df.withColumn("DATA_TYPE_ID", F.row_number().over(Window.orderBy("DATA_TYPE")))
    data_type_df = data_type_df.rename(F.col("DATA_TYPE"), "DATA_TYPE_NAME")
    data_type_df = data_type_df.select(F.col("DATA_TYPE_ID"), F.col("DATA_TYPE_NAME"))

    cancer_group_df = incidence_df.select(F.col("CANCER_GROUP_SITE")). \
                    unionAll(mortality_df.select(F.col("CANCER_GROUP_SITE"))). \
                    unionAll(survival_df.select(F.col("CANCER_GROUP_SITE"))). \
                    unionAll(incidence_territory_df.select(F.col("CANCER_GROUP_SITE"))).distinct()
    cancer_group_df = cancer_group_df.withColumn("CANCER_GROUP_ID", F.row_number().over(Window.orderBy("CANCER_GROUP_SITE")))
    cancer_group_df = cancer_group_df.rename(F.col("CANCER_GROUP_SITE"), "CANCER_GROUP_SITE_NAME")
    cancer_group_df = cancer_group_df.select(F.col("CANCER_GROUP_ID"), F.col("CANCER_GROUP_SITE_NAME"))

    sex_df = (incidence_df.select(F.col("SEX"))
              .unionAll(mortality_df.select(F.col("SEX")))
              .unionAll(survival_df.select(F.col("SEX")))
              .unionAll(incidence_territory_df.select(F.col("SEX")))
              .distinct())
    sex_df = sex_df.withColumn("SEX_ID", F.row_number().over(Window.orderBy("SEX")))
    sex_df = sex_df.rename(F.col("SEX"), "SEX_TYPE")
    sex_df = sex_df.select(F.col("SEX_ID"), F.col("SEX_TYPE"))

    age_group_df = (incidence_df.select(F.col("AGE_GROUP_YEARS"))
                    .unionAll(mortality_df.select(F.col("AGE_GROUP_YEARS")))
                    .unionAll(survival_df.select(F.col("AGE_GROUP_YEARS")))
                    .distinct())
    age_group_df = age_group_df.withColumn("AGE_GROUP_ID", F.row_number().over(Window.orderBy("AGE_GROUP_YEARS")))
    age_group_df = age_group_df.select(F.col("AGE_GROUP_ID"), F.col("AGE_GROUP_YEARS"))

    state_territory_df = incidence_territory_df.select(F.col("STATE_OR_TERRITORY")).distinct()
    state_territory_df = state_territory_df.withColumn("STATE_TERRITORY_ID", F.row_number().over(Window.orderBy("STATE_OR_TERRITORY")))
    state_territory_df = state_territory_df.rename(F.col("STATE_OR_TERRITORY"), "STATE_OR_TERRITORY_NAME")
    state_territory_df = state_territory_df.select(F.col("STATE_TERRITORY_ID"), F.col("STATE_OR_TERRITORY_NAME"))

    data_type_df.create_or_replace_view('DATA_TYPE_VIEW')
    cancer_group_df.create_or_replace_view('CANCER_GROUP_VIEW')
    sex_df.create_or_replace_view("SEX_VIEW")
    age_group_df.create_or_replace_view("AGE_GROUP_VIEW")
    state_territory_df.create_or_replace_view("STATE_TERRITORY_VIEW")

def create_views(session):
    session.use_schema('VIEWS')
    incidence_df = session.table("CANCERANALYTICS_DB.RAW_DATA.INCIDENCE")
    mortality_df = session.table("CANCERANALYTICS_DB.RAW_DATA.MORTALITY")
    survival_df = session.table("CANCERANALYTICS_DB.RAW_DATA.SURVIVAL")
    incidence_territory_df = session.table("CANCERANALYTICS_DB.RAW_DATA.INCIDENCE_TERRITORY")

    cancer_stats_view = mortality_df.join(incidence_df, 
                                          (mortality_df['YEAR'] == incidence_df['YEAR']) &\
                                          (mortality_df['SEX'] == incidence_df['SEX']) & \
                                          (mortality_df['AGE_GROUP_YEARS'] == incidence_df['AGE_GROUP_YEARS']) & \
                                          (mortality_df['CANCER_GROUP_SITE'] == incidence_df['CANCER_GROUP_SITE']),
                                          rsuffix = '_INCIDENCE').rename("COUNT", "COUNT_MORTALITY")
                                          
    cancer_stats_view = cancer_stats_view.select(F.col("YEAR"), \
                                               F.col("SEX"), \
                                               F.col("AGE_GROUP_YEARS"), \
                                               F.col("CANCER_GROUP_SITE"), \
                                               F.col("COUNT_MORTALITY"), \
                                               F.col("COUNT_INCIDENCE"))
    
    
    cancer_stats_view.create_or_replace_view('CANCER_STATS_VIEW')
    incidence_territory_df.create_or_replace_view('INCIDENCE_TERRITORY_VIEW')
    survival_df.create_or_replace_view('SURVIVAL_VIEW')


def create_views_stream(session):
    session.use_schema('VIEWS')

    # Stream for INCIDENCE_TERRITORY_VIEW
    _ = session.sql('CREATE OR REPLACE STREAM CANCER_STATS_VIEW_STREAM \
                        ON VIEW CANCER_STATS_VIEW \
                        SHOW_INITIAL_ROWS = TRUE').collect()
    
    # Stream for INCIDENCE_TERRITORY_VIEW
    _ = session.sql('CREATE OR REPLACE STREAM INCIDENCE_TERRITORY_VIEW_STREAM \
                        ON VIEW INCIDENCE_TERRITORY_VIEW \
                        SHOW_INITIAL_ROWS = TRUE').collect()

    # Stream for SURVIVAL_VIEW
    _ = session.sql('CREATE OR REPLACE STREAM SURVIVAL_VIEW_STREAM \
                        ON VIEW SURVIVAL_VIEW \
                        SHOW_INITIAL_ROWS = TRUE').collect()
    
def test_views(session):
    session.use_schema('VIEWS')
    cancer_stats = session.table('CANCER_STATS_VIEW')
    territory_stats = session.table('INCIDENCE_TERRITORY_VIEW')
    survival_stats = session.table('SURVIVAL_VIEW')
    cancer_stats.limit(5).show()   
    territory_stats.limit(5).show()  
    survival_stats.limit(5).show()  

# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    create_dimension_views(session, raw_schema='RAW_DATA', transformed_schema='TRANSFORMED_DATA')
    # create_views_stream(session)
    # test_views(session)

    session.close()