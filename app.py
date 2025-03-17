import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="wide"
)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Retrieve Snowflake credentials from Streamlit secrets
try:
    snowflake_config = st.secrets["snowflake"]
    account = snowflake_config["account"]
    user = snowflake_config["user"]
    password = snowflake_config["password"]
    warehouse = snowflake_config["warehouse"]
    database = snowflake_config["database"]
    schema = snowflake_config["schema"]
    st.success("✅ Successfully retrieved Snowflake credentials from secrets.")
except KeyError as e:
    st.error(f"❌ Failed to retrieve Snowflake connection details from secrets: {e}")
    st.stop()

# Establish Snowflake session
session = get_active_session()

if session is None:
    st.error("Unable to establish a Snowflake session. Please ensure you are running this app within a Snowflake environment.")
    st.stop()

# Function to fetch data based on the table name
def fetch_data(table_name):
    try:
        df = session.table(table_name).to_pandas()
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch override ref data based on the selected module
def fetch_override_ref_data(selected_module=None):
    try:
        df = session.table("Override_Ref").to_pandas()
        df.columns = [col.upper() for col in df.columns]

        # Filter based on the selected module if provided
        if selected_module:
            module_num = int(selected_module.split('-')[1])
            df = df[df['MODULE'] == module_num]
        return df
    except Exception as e:
        st.error(f"Error fetching data from Override_Ref: {e}")
        return pd.DataFrame()

# Function to update a row in the source table
def update_source_table_row(source_table, primary_key_values, editable_column, new_value):
    try:
        # Construct the UPDATE statement dynamically based on primary key columns
        where_clause = " AND ".join([f"{col} = '{val}'" for col, val in primary_key_values.items()])
        update_sql = f"""
            UPDATE {source_table}
            SET {editable_column} = '{new_value}'
            WHERE {where_clause}
        """

        # Execute the UPDATE statement
        session.sql(update_sql).collect()

    except Exception as e:
        st.error(f"Error updating row in {source_table}: {e}")

# Function to insert a row in the target table with RECORD_FLAG = 'O'
def insert_into_target_table(target_table, row_data, editable_column, new_value):
    try:
        # Construct the INSERT statement dynamically
        columns = ", ".join(row_data.keys())
        values = ", ".join([f"'{val}'" for val in row_data.values()])

        insert_sql = f"""
            INSERT INTO {target_table} ({columns}, AS_AT_DATE, RECORD_FLAG)
            VALUES ({values}, CURRENT_TIMESTAMP(), 'O')
        """

        session.sql(insert_sql).collect()

    except Exception as e:
        st.error(f"Error inserting values into {target_table}: {e}")

# Main app
# List available modules - Dynamically populate from Override_Ref
override_ref_df = fetch_data("Override_Ref")
if not override_ref_df.empty:
    module_numbers = sorted(override_ref_df['MODULE'].unique())
    available_modules = [f"Module-{int(module)}" for module in module_numbers]
else:
    available_modules = []
    st.warning("No modules found in Override_Ref table.")

# Select module
selected_module = st.selectbox("Select Module", available_modules)

# Get tables for the selected module
module_tables_df = fetch_override_ref_data(selected_module)

if not module_tables_df.empty:
    available_tables = module_tables_df['SOURCE_TABLE'].unique()

    # Select table within the module
    selected_table = st.selectbox("Select Table", available_tables)

    # Filter Override_Ref data based on the selected table
    table_info_df = module_tables_df[module_tables_df['SOURCE_TABLE'] == selected_table]

    if not table_info_df.empty:
        target_table_name = table_info_df['TARGET_TABLE'].iloc[0]
        # Fetch editable columns from table_info_df
        editable_columns = table_info_df['EDITABLE_COLUMN'].unique()

        # Select editable column
        selected_column = st.selectbox("Editable_column", editable_columns)
        selected_column_upper = selected_column.upper()

        # Fetch primary key columns dynamically from source table
        primary_key_cols = []
        if selected_table == 'fact_portfolio_perf':
            primary_key_cols = ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY']
        elif selected_table == 'fact_income':
            primary_key_cols = ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY']
        elif selected_table == 'fact_msme':
            primary_key_cols = ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY']
        elif selected_table == 'fact_orders':
            primary_key_cols = ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY']
        elif selected_table == 'fact_customers':
            primary_key_cols = ['AS_OF_DATE', 'PORTFOLIO', 'PORTFOLIO_SEGMENT', 'CATEGORY']
        else:
            st.error("Primary key columns not defined for this table. Please update the code.")
            st.stop()

        # Split the data into two tabs
        tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

        with tab1:
            st.subheader(f"Source Data from {selected_table}")

            # Fetch data at the beginning
            source_df = fetch_data(selected_table)
            if not source_df.empty:
                # Make the dataframe editable using st.data_editor
                edited_df = source_df.copy()

                # Apply a background color to the editable column
                def highlight_editable_column(df, column_name):
                    # Create a style for the entire dataframe
                    styled_df = pd.DataFrame('', index=df.index, columns=df.columns)
                    # Apply yellow background only to the selected column
                    styled_df[column_name] = 'background-color: #FFFFE00'
                    return styled_df

                # Disable editing for all columns except the selected editable column
                disabled_cols = [col for col in edited_df.columns if col != selected_column_upper]

                styled_df = edited_df.style.apply(highlight_editable_column, column_name=selected_column_upper, axis=None)

                edited_df = st.data_editor(
                    styled_df,  # Pass the styled dataframe
                    key=f"data_editor_{selected_table}_{selected_column}",
                    num_rows="dynamic",
                    use_container_width=True,
                    disabled=disabled_cols
                )

                # Submit button to update the source table and insert to the target table
                if st.button("Submit Updates"):
                    try:
                        # Identify rows that have been edited
                        changed_rows = edited_df[edited_df[selected_column_upper] != source_df[selected_column_upper]]

                        if not changed_rows.empty:
                            for index, row in changed_rows.iterrows():
                                # Extract primary key values
                                primary_key_values = {col: row[col] for col in primary_key_cols}

                                # Get new value for the selected column
                                new_value = row[selected_column_upper]

                                # Update source table
                                update_source_table_row(selected_table, primary_key_values, selected_column, new_value)

                                # Prepare row data for insertion into target table
                                row_data = row.to_dict()

                                # Insert into target table
                                insert_into_target_table(target_table_name, row_data, selected_column, new_value)

                            st.success("Data updated successfully!")
                        else:
                            st.info("No changes were made.")

                    except Exception as e:
                        st.error(f"Error during update/insert: {e}")
            else:
                st.info(f"No data available in {selected_table}.")

        with tab2:
            st.subheader(f"Overridden Values from {target_table_name}")

            # Fetch overridden data (ONLY the latest overrides)
            override_df = fetch_data(target_table_name)
            if not override_df.empty:
                st.dataframe(override_df, use_container_width=True)
            else:
                st.info(f"No overridden data available in {target_table_name}.")

    else:
        st.warning("No table information found in Override_Ref for the selected table.")

else:
    st.warning("No tables found for the selected module in Override_Ref table.")
