
import pandas as pd
import functions.basic_functions as bf
import numpy as np

def get_vendor_data(input_data):
    vendor_capacity = pd.read_excel(input_data, sheet_name='vendor inputs',).iloc[:3, :]
    vendor_capacity = vendor_capacity.set_index('name')
    vendor_cohort = pd.read_excel(input_data, sheet_name='vendor inputs', header=6, usecols="A:D").iloc[:12, :]
    
    vendor_cohort_st = pd.read_excel(input_data, sheet_name='vendor inputs', header=24, usecols="A:C")
    vendor_cohort_st = vendor_cohort_st.set_index('Service Territory')

    nsa_df = vendor_cohort[vendor_cohort['vendor']=='NSA']
    dish_df = vendor_cohort[vendor_cohort['vendor']=='DISH']

    nsa_df = nsa_df.pivot_table(index=['cohort', 'WO Type'], values=['capacity', 'WO Type'], aggfunc={'capacity': np.sum}).reset_index()
    nsa_df.columns = ['NSA cohort', 'WO Type', 'capacity']

    dish_df = dish_df.pivot_table(index=['cohort', 'WO Type'], values=['capacity', 'WO Type'], aggfunc={'capacity': np.sum}).reset_index()
    dish_df.columns = ['Dish cohort', 'WO Type', 'capacity']

    nsa_vendor_cohort_st = vendor_cohort_st.reset_index().merge(nsa_df, how='left', on='NSA cohort').set_index('Service Territory')
    dish_vendor_cohort_st = vendor_cohort_st.reset_index().merge(dish_df, how='left', on='Dish cohort').set_index('Service Territory')

    return vendor_capacity, nsa_vendor_cohort_st, dish_vendor_cohort_st

def get_install_implementation_data(input_data):
    install_backlog_df = pd.read_excel(input_data, sheet_name='installs',)
    install_backlog_df = install_backlog_df.set_index('Service Territory')
    install_backlog_df.columns = ['age_month', 'ct']

    locs_in_implementation_df = pd.read_excel(input_data, sheet_name='implementation',)
    locs_in_implementation_df = locs_in_implementation_df.set_index('Service Territory')
    locs_in_implementation_df.columns = ['age_month', 'ct']

    return install_backlog_df, locs_in_implementation_df


def get_live_fleet_data(input_data, st_df):
    live_fleet_df = pd.read_excel(input_data, sheet_name='live_fleet',)
    live_fleet_df = live_fleet_df.set_index('Service Territory')
    live_fleet_df.columns = ['live_fleet_cnt', 'ttl_fleet', 'live_fleet_perc']
    live_fleet_df = bf.align_st(live_fleet_df, st_df)
    live_fleet_df = live_fleet_df.fillna(0)

    return live_fleet_df


def get_single_inputs(input_data):
    single_inputs = pd.read_excel(input_data, sheet_name='single inputs',)
    single_inputs = single_inputs.set_index('Type')

    return single_inputs

def get_monthly_data(input_data, st_df):
    monthly_inputs = pd.read_excel(input_data, sheet_name='monthly inputs',).iloc[:4, :]
    monthly_inputs = monthly_inputs.set_index('name')

    winter_maint_mom = pd.read_excel(input_data, sheet_name='monthly inputs',header=8,)
    winter_maint_mom = winter_maint_mom.set_index('Service Territory')
    winter_maint_mom = bf.align_st(winter_maint_mom, st_df)
    winter_maint_mom = winter_maint_mom.fillna(0)
    winter_maint_mom.columns = bf.convert_list_date_cols_to_str(winter_maint_mom)

    return monthly_inputs, winter_maint_mom

def get_sales_data(input_data, st_df):
    sales_funnel_sla_df = pd.read_excel(input_data, sheet_name='sales_funnel', header=2).iloc[:4, :]
    sales_funnel_sla_df = sales_funnel_sla_df.set_index('WO created')

    new_sales_sla = pd.read_excel(input_data, sheet_name='sales_funnel', header=8, usecols="A:B",).iloc[:1, :]
    new_sales_sla = new_sales_sla.set_index('Type')

    sales_distb = pd.read_excel(input_data, sheet_name='sales_funnel', header=11,).iloc[:, :]
    sales_distb = sales_distb.set_index('Service Territory')
    sales_distb = bf.align_st(sales_distb, st_df)
    sales_distb = sales_distb.fillna(0)

    return sales_funnel_sla_df, new_sales_sla, sales_distb

def get_initial_tech_counts(input_data, st_df):
    initial_tech_count = pd.read_excel(input_data, sheet_name='initial_tech_count',)
    initial_tech_count = initial_tech_count.set_index('Service Territory')
    initial_tech_count = bf.align_st(initial_tech_count, st_df)
    initial_tech_count = initial_tech_count.fillna(0)

    initial_local_tech_count=initial_tech_count[['1P tech count']]
    initial_travel_tech_count=initial_tech_count[['Travel tech count']]

    return initial_local_tech_count, initial_travel_tech_count

def get_local_tech_hires(input_data, st_df):
    max_local_tech_hires = pd.read_excel(input_data, sheet_name='local_tech_hires',).fillna(0)
    max_local_tech_hires = max_local_tech_hires.set_index('Service Territory').iloc[0]
    max_local_tech_hires = bf.convert_date_idx_to_str(max_local_tech_hires)

    local_tech_hires = pd.read_excel(input_data, sheet_name='local_tech_hires',).iloc[1:, :]
    local_tech_hires = local_tech_hires.set_index('Service Territory')
    local_tech_hires = bf.align_st(local_tech_hires, st_df)
    local_tech_hires.columns = bf.convert_list_date_cols_to_str(local_tech_hires)
    local_tech_hires = local_tech_hires.fillna(0)

    return max_local_tech_hires, local_tech_hires