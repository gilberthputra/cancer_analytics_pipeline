from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

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

    create_views(session)
    create_views_stream(session)
    test_views(session)

    session.close()