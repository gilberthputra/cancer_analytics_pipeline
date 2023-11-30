import time
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def load_all_dimension_table(session):
    session.use_schema("TRANSFORMED_DATA")
    age_group_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.AGE_GROUP")
    cancer_group_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.CANCER_GROUP")
    data_type_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.DATA_TYPE")
    sex_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.SEX")
    state_territory_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.STATE_TERRITORY")

    return {"age_group": age_group_df, "cancer_group": cancer_group_df, 
            "data_type": data_type_df, "sex": sex_df, 
            "state_territory": state_territory_df}

def create_cancer_incidence_view(session, dim_table):
    session.use_schema("TRANSFORMED_DATA")

    age_group_df = dim_table['age_group']
    cancer_group_df = dim_table['cancer_group']
    data_type_df = dim_table['data_type']
    sex_df = dim_table['sex']

    incidence_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE")

    incidence_view = incidence_df.join(age_group_df, incidence_df['AGE_GROUP_ID'] == age_group_df['AGE_GROUP_ID']) \
                                .join(cancer_group_df, incidence_df["CANCER_GROUP_ID"] == cancer_group_df["CANCER_GROUP_ID"]) \
                                .join(sex_df, incidence_df["SEX_ID"] == sex_df["SEX_ID"]) \
                                .join(data_type_df, incidence_df["DATA_TYPE_ID"] == data_type_df["DATA_TYPE_ID"]) \
                                .select(F.col("DATA_TYPE_NAME"),
                                        F.col("CANCER_GROUP_SITE_NAME"),
                                        F.col("AGE_GROUP_YEARS"),
                                        F.col("SEX_TYPE"),
                                        F.col("YEAR"),
                                        F.col("COUNT"))
    incidence_view.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE_VIEW")

def create_cancer_mortality_view(session, dim_table):
    session.use_schema("TRANSFORMED_DATA")

    age_group_df = dim_table['age_group']
    cancer_group_df = dim_table['cancer_group']
    data_type_df = dim_table['data_type']
    sex_df = dim_table['sex']

    mortality_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.MORTALITY")

    mortality_view = mortality_df.join(age_group_df, mortality_df['AGE_GROUP_ID'] == age_group_df['AGE_GROUP_ID']) \
                                .join(cancer_group_df, mortality_df["CANCER_GROUP_ID"] == cancer_group_df["CANCER_GROUP_ID"]) \
                                .join(sex_df, mortality_df["SEX_ID"] == sex_df["SEX_ID"]) \
                                .join(data_type_df, mortality_df["DATA_TYPE_ID"] == data_type_df["DATA_TYPE_ID"]) \
                                .select(F.col("DATA_TYPE_NAME"),
                                        F.col("CANCER_GROUP_SITE_NAME"),
                                        F.col("AGE_GROUP_YEARS"),
                                        F.col("SEX_TYPE"),
                                        F.col("YEAR"),
                                        F.col("COUNT"))
    mortality_view.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.MORTALITY_VIEW")

def create_cancer_survival_view(session, dim_table):
    session.use_schema("TRANSFORMED_DATA")

    age_group_df = dim_table['age_group']
    cancer_group_df = dim_table['cancer_group']
    data_type_df = dim_table['data_type']
    sex_df = dim_table['sex']

    survival_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.SURVIVAL")

    survival_view = survival_df.join(age_group_df, survival_df['AGE_GROUP_ID'] == age_group_df['AGE_GROUP_ID']) \
                                .join(cancer_group_df, survival_df["CANCER_GROUP_ID"] == cancer_group_df["CANCER_GROUP_ID"]) \
                                .join(sex_df, survival_df["SEX_ID"] == sex_df["SEX_ID"]) \
                                .join(data_type_df, survival_df["DATA_TYPE_ID"] == data_type_df["DATA_TYPE_ID"]) \
                                .select(F.col("DATA_TYPE_NAME"),
                                        F.col("CANCER_GROUP_SITE_NAME"),
                                        F.col("AGE_GROUP_YEARS"),
                                        F.col("SEX_TYPE"),
                                        F.col("PERIOD_YEARS"),
                                        F.col("YEARS_AFTER_DIAGNOSIS"),
                                        F.col("SURVIVAL"),
                                        F.col("95_CI_LOWER_BOUND"),
                                        F.col("95_CI_UPPER_BOUND"))
    survival_view.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.SURVIVAL_VIEW")

def create_cancer_territory_view(session, dim_table):
    session.use_schema("TRANSFORMED_DATA")

    territory_df = dim_table['state_territory']
    cancer_group_df = dim_table['cancer_group']
    data_type_df = dim_table['data_type']
    sex_df = dim_table['sex']

    inc_territory_df = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE_TERRITORY")

    inc_territory_view = inc_territory_df.join(territory_df, inc_territory_df['STATE_TERRITORY_ID'] == territory_df['STATE_TERRITORY_ID']) \
                                .join(cancer_group_df, inc_territory_df["CANCER_GROUP_ID"] == cancer_group_df["CANCER_GROUP_ID"]) \
                                .join(sex_df, inc_territory_df["SEX_ID"] == sex_df["SEX_ID"]) \
                                .join(data_type_df, inc_territory_df["DATA_TYPE_ID"] == data_type_df["DATA_TYPE_ID"]) \
                                .select(F.col("DATA_TYPE_NAME"),
                                        F.col("CANCER_GROUP_SITE_NAME"),
                                        F.col("STATE_OR_TERRITORY_NAME"),
                                        F.col("SEX_TYPE"),
                                        F.col("YEAR"),
                                        F.col("COUNT"))
    inc_territory_view.create_or_replace_view("CANCERANALYTICS_DB.TRANSFORMED_DATA.INC_TERRITORY_VIEW")

def main(session: Session) -> str:
    dim_tables = load_all_dimension_table(session)
    create_cancer_incidence_view(session, dim_tables)
    create_cancer_mortality_view(session, dim_tables)
    create_cancer_survival_view(session, dim_tables)
    create_cancer_territory_view(session, dim_tables)

    return f"Successfully created or replaced"

def test_views(session):
    session.use_schema("TRANSFORMED_DATA")

    incidence = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.INCIDENCE_VIEW")
    mortality = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.MORTALITY_VIEW")
    survival = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.SURVIVAL_VIEW")
    territory = session.table("CANCERANALYTICS_DB.TRANSFORMED_DATA.INC_TERRITORY_VIEW")

    incidence.show(5)
    mortality.show(5)
    survival.show(5)
    territory.show(5)

    print(incidence.count())
    print(mortality.count())
    print(survival.count())
    print(territory.count())

if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    test_views(session)

    session.close()