import pandas as pd
import math
import input_file_functions as iff
import basic_functions as bf

num_wks_in_month = 4.33

# not sure if 3 lines below are needed
# num_months_val = flight_tech_df.loc['Number of months', 'Value']
# new_sales_sla_val = new_sales_sla.loc['New Sales SLA (months)', 'Value']
# roll_install_dt_unconstrained = get_rolling_df(install_dt_unconstrained_w_backlog).iloc[:, 1:]


def get_wo_tech_mnthly_rr_less_ss(flight_tech_df, monthly_inputs):
    install_with_ss_perc_val = flight_tech_df.loc['Installs w/ SS perc', 'Value']
    ss_perc_3p_val = flight_tech_df.loc['SS perc 3P', 'Value']

    wo_tech_wkyl_rr = monthly_inputs.loc['WO / tech / week', :]

    wo_tech_wkyl_rr_less_ss = wo_tech_wkyl_rr * (1-(install_with_ss_perc_val*(1-ss_perc_3p_val)))
    wo_tech_wkyl_rr_less_ss = wo_tech_wkyl_rr_less_ss.apply(lambda x: math.ceil(x))

    wo_tech_mnthly_rr_less_ss = wo_tech_wkyl_rr_less_ss * num_wks_in_month
    wo_tech_mnthly_rr_less_ss = wo_tech_mnthly_rr_less_ss.apply(lambda x: math.ceil(x))

    wo_tech_mnthly_rr_less_ss = bf.convert_date_idx_to_str(wo_tech_mnthly_rr_less_ss)

    return wo_tech_mnthly_rr_less_ss

def get_install_perc_cap_input(monthly_inputs):
    install_perc_cap_input = monthly_inputs.loc['perc capacity towards installs (3P)']
    install_perc_cap_input = bf.convert_date_idx_to_str(install_perc_cap_input)

    return install_perc_cap_input

def get_implementation_wo_backlog(locs_in_implementation_df, st_df):

    locs_in_implementation_df_c = locs_in_implementation_df.copy()
    implementation_backlog_gb_wo = locs_in_implementation_df_c.groupby(['Service Territory', 'age_month']).sum('ct').reset_index()

    implementation_wo_backlog_piv = implementation_backlog_gb_wo.pivot(index='Service Territory', columns='age_month', values='ct').reset_index()
    implementation_wo_backlog_piv = implementation_wo_backlog_piv.set_index('Service Territory')
    implementation_wo_backlog_piv = bf.align_st(implementation_wo_backlog_piv, st_df)
    implementation_wo_backlog_piv.fillna(0, inplace=True)

    return implementation_wo_backlog_piv

def get_install_wo_backlog(install_backlog_df, locs_in_implementation_df, st_df):
    install_backlog_sql_c = install_backlog_df.copy()
    install_backlog_gb_wo = install_backlog_sql_c.groupby(['Service Territory', 'age_month']).sum('ct').reset_index()

    install_wo_backlog_piv = install_backlog_gb_wo.pivot(index='Service Territory', columns='age_month', values='ct').reset_index()
    install_wo_backlog_piv = install_wo_backlog_piv.set_index('Service Territory')
    install_wo_backlog_piv = bf.align_st(install_wo_backlog_piv, st_df)
    install_wo_backlog_piv.fillna(0, inplace=True)

    implementation_wo_backlog_piv = get_implementation_wo_backlog(locs_in_implementation_df, st_df)
    install_wo_backlog_piv = pd.concat([install_wo_backlog_piv, implementation_wo_backlog_piv]).groupby('Service Territory').sum().reset_index()
    install_wo_backlog_piv = install_wo_backlog_piv.set_index('Service Territory')

    install_wo_backlog_col_list = list(install_wo_backlog_piv.columns)
    install_wo_backlog_col_list = [str(x)[:10] for x in install_wo_backlog_col_list]
    install_wo_backlog_piv.columns = install_wo_backlog_col_list
    install_wo_backlog_piv = install_wo_backlog_piv.groupby(install_wo_backlog_piv.columns, axis=1).sum()

    return install_wo_backlog_piv

def get_sales_funnel_sla(main_col_list, sales_funnel_sla_df, sales_distb):
    sales_dt = sales_funnel_sla_df.copy()
    sales_dt = sales_dt.sum()

    sales_funnel_distb_c = sales_distb.copy()

    sales_funnel_sla = sales_funnel_distb_c * sales_dt
    sales_funnel_sla_col_list = list(sales_funnel_sla.columns)
    sales_funnel_sla_col_list = [str(x)[:10] for x in sales_funnel_sla_col_list]

    sales_funnel_sla.columns = sales_funnel_sla_col_list

    sales_funnel_sla = sales_funnel_sla[main_col_list].fillna(0)
    sales_funnel_sla = sales_funnel_sla.applymap(lambda x: x if x%.5!=0 else x+.1)
    sales_funnel_sla = sales_funnel_sla.round(0) # rounds odd unit values up, and even unit values down for .5

    return sales_funnel_sla

def get_install_unconstrained(main_col_list, sales_funnel_sla_df, sales_distb, install_backlog_df, locs_in_implementation_df, st_df):
    backlog_date = '2024-07-01'

    sales_funnel_sla = get_sales_funnel_sla(main_col_list, sales_funnel_sla_df, sales_distb)
    install_wo_backlog_piv = get_install_wo_backlog(install_backlog_df, locs_in_implementation_df, st_df)

    install_dt_unconstrained_full = pd.concat([sales_funnel_sla, install_wo_backlog_piv]).reset_index().groupby('Service Territory', sort=False).sum(min_count=1)
    install_dt_unconstrained_full.sort_index(axis=1, inplace=True)

    install_dt_unconstrained_full_cum = install_dt_unconstrained_full.cumsum(axis=1)
    install_backlog_ser = install_dt_unconstrained_full_cum[backlog_date]

    install_dt_unconstrained = install_dt_unconstrained_full.loc[:,main_col_list[0]:main_col_list[-1]]
    install_dt_unconstrained_w_backlog = pd.concat([install_backlog_ser, install_dt_unconstrained],  axis=1)

    return install_backlog_ser, install_dt_unconstrained, install_dt_unconstrained_w_backlog

def get_maint_creation(monthly_inputs):
    maint_creation_df = monthly_inputs.loc['maint creation % of fleet',:]
    maint_creation_df.columns = bf.convert_date_idx_to_str(maint_creation_df)

    # init_live_fleet = float(live_fleet_df['ttl_fleet'].iloc[0])
    # init_st_mnthly_live_fleet_perc = live_fleet_df['live_fleet_perc'].astype(float)

    return maint_creation_df
