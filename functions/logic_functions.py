import pandas as pd
import numpy as np
import math

import warnings
warnings.filterwarnings("ignore")

def get_current_month_local_tech_supply(qtrly_tech_count_ser, rem_inst_ser, monthly_tech_rr, dt_col, install_perc_cap_input):
    
    cur_mon_local_tech_supply_ser_list = []
    for serv_tr, v in rem_inst_ser.items():

        rem_inst_ser = rem_inst_ser.astype(float)
        st_demand = rem_inst_ser.get(key = serv_tr)

        st_qtrly_tech_count = qtrly_tech_count_ser[qtrly_tech_count_ser.index == serv_tr][0]
        st_qtrly_tech_cap = st_qtrly_tech_count * monthly_tech_rr
        st_qtrly_tech_cap = st_qtrly_tech_cap * install_perc_cap_input.loc[dt_col]
        
        st_qtrly_tech_cap =  st_qtrly_tech_cap if st_qtrly_tech_cap%.5!=0 else st_qtrly_tech_cap + .1

        st_qtrly_tech_cap = round(st_qtrly_tech_cap)       

        st_cur_mon_local_tech_supply = st_qtrly_tech_cap if st_demand >= st_qtrly_tech_cap else st_demand
        
        cur_mon_local_tech_supply_ser_list.append((serv_tr, st_cur_mon_local_tech_supply)) 

    idx, values = zip(*cur_mon_local_tech_supply_ser_list) 
    cur_mon_local_tech_supply_ser = pd.Series(values, idx) # column i4
    cur_mon_local_tech_supply_ser.rename(dt_col, inplace=True)

    return cur_mon_local_tech_supply_ser

def round_down(num, divisor):
    return math.floor(num / divisor) * divisor

def techs_needed_to_complete_work(current_month_install_dt_ser, cur_mon_local_tech_supply_ser,monthly_tech_rr, itr, max_travel_tech_cap=None):
    cur_mon_techs_need_ser = (current_month_install_dt_ser - cur_mon_local_tech_supply_ser)/ monthly_tech_rr
    cur_mon_techs_need_ser.fillna(0, inplace=True)

    cur_mon_techs_need_ser = cur_mon_techs_need_ser.apply(lambda x: round_down(x, .05))

    if max_travel_tech_cap:
        cur_mon_techs_need_ser = cur_mon_techs_need_ser.apply(lambda x: min(max_travel_tech_cap, x))
    
    cur_mon_techs_need_ser.rename_axis("Service Territory", inplace=True)
    cur_mon_techs_need_ser = cur_mon_techs_need_ser.astype(float)

    cur_mon_techs_need_ser_ranked = cur_mon_techs_need_ser.rank(method='first', ascending=False)
    cur_mon_techs_need_ser_rank_x_sort = cur_mon_techs_need_ser_ranked.sort_values()

    return cur_mon_techs_need_ser, 'delete cur_mon_techs_need_ser_rank_x_sort'

def constrain_tech_needs(cur_mon_excess_tech_needs_ser, st_ttl_travel_tech_cnt_ser, monthly_tech_rr, dt_col, install_perc_cap_input):

    cur_mon_excess_tech_needs_cum_sum_ser = cur_mon_excess_tech_needs_ser.sort_values(ascending=False).cumsum()

    cur_mon_excess_tech_needs_df = pd.concat([cur_mon_excess_tech_needs_ser, cur_mon_excess_tech_needs_cum_sum_ser], axis=1)
    cur_mon_excess_tech_needs_df.columns = ['need', 'cum_sum']

    monthly_flight_cap = st_ttl_travel_tech_cnt_ser.sum() * monthly_tech_rr
    monthly_flight_cap = round(monthly_flight_cap * install_perc_cap_input[dt_col]) # rounds odd unit values up, and even unit values down for .5

    num_travel_techs = round(monthly_flight_cap/monthly_tech_rr) # rounds odd unit values up, and even unit values down for .5

    cur_mon_excess_tech_needs_df['need_delta'] = num_travel_techs - cur_mon_excess_tech_needs_df['cum_sum']

    cur_mon_excess_tech_needs_df['overage'] = cur_mon_excess_tech_needs_df['need'] + cur_mon_excess_tech_needs_df['need_delta']
    cur_mon_excess_tech_needs_df['overage'] = cur_mon_excess_tech_needs_df['overage'].apply(lambda x: max(x, 0))

    cur_mon_excess_tech_needs_df['final'] = \
        np.where(cur_mon_excess_tech_needs_df['need_delta'] > 0, cur_mon_excess_tech_needs_df['need'], cur_mon_excess_tech_needs_df['overage'])

    cur_mon_travel_tech_supply_ser = cur_mon_excess_tech_needs_df['final'].apply(lambda x: math.floor(x * monthly_tech_rr))
    cur_mon_travel_tech_supply_ser.rename(dt_col, inplace=True)

    return cur_mon_travel_tech_supply_ser

def constrain_tech_needs_maint(cur_mon_excess_tech_needs_ser, temp_list, monthly_tech_rr, dt_col, itr):
    ser_cum_sum = cur_mon_excess_tech_needs_ser.sort_values(ascending=False).cumsum()
    ser_df = pd.concat([cur_mon_excess_tech_needs_ser, ser_cum_sum], axis=1)
    ser_df.columns = ['val', 'cum_sum']

    ser_df['delta'] = float(temp_list) - ser_df['cum_sum']
    ser_df['overage'] = ser_df['val'] + ser_df['delta']

    ser_df['overage'] = ser_df['overage'].apply(lambda x: max(x, 0))

    ser_df['final'] = np.where(ser_df['delta'] > 0, ser_df['val'], ser_df['overage'])

    cur_mon_travel_tech_supply_ser = ser_df['final']
    cur_mon_travel_tech_supply_ser = cur_mon_travel_tech_supply_ser.sort_values()
    cur_mon_travel_tech_supply_ser = cur_mon_travel_tech_supply_ser.apply(lambda x: math.floor(x * monthly_tech_rr))

    cur_mon_travel_tech_supply_ser.rename(dt_col, inplace=True)

    return cur_mon_travel_tech_supply_ser

def get_remaining_install_dt(remaining_install_dt_ser, current_month_local_tech_supply_ser, current_month_travel_tech_supply_ser, current_month_install_dt_ser, dt_col):

    df_test = pd.concat([remaining_install_dt_ser, current_month_local_tech_supply_ser, current_month_travel_tech_supply_ser, current_month_install_dt_ser], axis=1)
    df_test.columns = ['remaining_install_dt', 'current_month_local_tech_supply', 'current_month_travel_tech_supply', 'current_month_install_dt']

    df_test['labor_supply'] = df_test['current_month_local_tech_supply'] + df_test['current_month_travel_tech_supply']
    # df_test['surplus_install_dt'] = df_test['remaining_install_dt'] - df_test['labor_supply'] + df_test['current_month_install_dt']
    df_test['surplus_install_dt'] = df_test['remaining_install_dt'] - df_test['labor_supply']
    df_test['surplus_install_dt'] = df_test['surplus_install_dt'].apply(lambda x: max(0,x))

    surplus_install_dt_ser = df_test['surplus_install_dt']
    surplus_install_dt_ser.rename(dt_col,inplace=True)

    return surplus_install_dt_ser

def get_new_tech_hire_list(surplus_install_dt_ser, max_hires_val, st_overwrite_ser, monthly_tech_rr):
    # remove st with values greater than zero in hiring table and reduce max_hires_val, then add back in at the end
    st_overwrites_list = list(st_overwrite_ser.index.unique()) # st's to remove
    surplus_install_dt_ser = surplus_install_dt_ser[~surplus_install_dt_ser.index.isin(st_overwrites_list)] # remove st from this ser

    techs_for_remaining_installs_ser = surplus_install_dt_ser / monthly_tech_rr
    techs_for_remaining_installs_ser.fillna(0, inplace=True)
    techs_for_remaining_installs_ser = techs_for_remaining_installs_ser.apply(math.floor)

    remaining_installs_rank = surplus_install_dt_ser.rank(method='first', ascending=False)
    techs_for_remaining_installs_ser = techs_for_remaining_installs_ser.apply(lambda x: min(x, 3))
    remaining_installs_rank.sort_values(inplace=True)

    df_aa = pd.concat([techs_for_remaining_installs_ser, remaining_installs_rank], axis=1)
    df_aa.columns = ['tech', 'rank']

    df_aa.sort_values(by=['rank'], inplace=True)
    df_aa['cum_sum'] = df_aa['tech'].cumsum()
    # df_aa['net_adds_list_itr'] = net_adds_list[net_adds_list_itr]
    # df_aa['net_adds_delta'] = net_adds_list[net_adds_list_itr] - df_aa['cum_sum']
    df_aa['net_adds_delta'] = max_hires_val - df_aa['cum_sum']
    df_aa['overage'] = df_aa['tech'] + df_aa['net_adds_delta']
    df_aa['overage'] = df_aa['overage'].apply(lambda x: max(x, 0))

    df_aa['final'] = np.where(df_aa['net_adds_delta'] > 0, df_aa['tech'], df_aa['overage'])

    res_ser = df_aa['final'].combine(st_overwrite_ser, max, fill_value=0)
    
    return res_ser

def update_tech_cnt_by_cap_ser(tech_cnt_by_cap_ser_c, local_tech_hire_ser, monthly_tech_rr, dt_col):
        
    sum_df = pd.concat([tech_cnt_by_cap_ser_c, local_tech_hire_ser], axis=1)
    sum_df.fillna(0, inplace=True)
    sum_df.columns = ['tech_cnt_by_cap_ser_col', 'final']
    sum_df['mnthly_wo'] = sum_df['final'] * monthly_tech_rr
    # sum_df['adds'] = sum_df['tech_cnt_by_cap_ser_col'] + sum_df['mnthly_wo']
    sum_df['adds'] = sum_df['tech_cnt_by_cap_ser_col'] + sum_df['final'] # was monthly_wo
    
    sum_df[dt_col] = sum_df['tech_cnt_by_cap_ser_col'] + sum_df['final'] # was monthly_wo
    # tech_cnt_by_cap_ser_c_list.append(sum_df[dt_col])
    
    new_tech_cnt_by_cap_ser = sum_df['adds']

    return new_tech_cnt_by_cap_ser

###################################################################################

###################################################################################


def capacity_constraint(in_df, needed_capcity, max_capacity, res_name):
    df = in_df.copy()
    df['cumsum'] = df[needed_capcity].cumsum()
    df['delta'] = df[max_capacity] - df['cumsum']
    df['overage'] = df[needed_capcity] + df['delta']
    df['overage'] = df['overage'].apply(lambda x: max(x, 0))

    df[res_name] = np.where(df['delta'] > 0, df[needed_capcity], df['overage'])
    df = df[[res_name]]

    res_df = pd.concat([in_df, df], axis=1)
    return res_df

def constrain_external_tech_needs(cur_mon_excess_external_tech_needs_ser, vendor_cohort_st, vendor, monthly_tech_rr, dt_col, vendor_maint_budget_cap, vendor_install_budget_cap, rem_max_val=None):
    all_vendor_df = pd.concat([cur_mon_excess_external_tech_needs_ser, vendor_cohort_st], axis=1)

    cohort_list = list(all_vendor_df[vendor].unique())

    cohort_df_list = []
    for cohort in cohort_list:
        
        cohort_df = all_vendor_df[all_vendor_df[vendor]==cohort]

        cohort_df = cohort_df[[0, 'capacity']].sort_values(by=[0], ascending=False)
        cohort_df['needed_cap'] = cohort_df[0] * monthly_tech_rr
        cohort_df['needed_cap'] = cohort_df['needed_cap'].apply(lambda x: round(x)) # change to regular round later
    
        cohort_df = capacity_constraint(cohort_df, 'needed_cap', 'capacity', 'cohort_capacity')

        # cohort_df = cohort_df[cohort_df['cohort_capacity'] > 0]
        cohort_df = cohort_df[['cohort_capacity']].reset_index()

        cohort_df['cohort'] = cohort

        cohort_df_list.append(cohort_df)

    all_cohort_df = pd.concat(cohort_df_list, ignore_index=True, sort=False)
    all_cohort_df = all_cohort_df.sort_values(by=['cohort_capacity'], ascending=False)

    if rem_max_val is not None:
        max_capacity_val = vendor_maint_budget_cap.loc[dt_col] - rem_max_val
    else:
        max_capacity_val = vendor_install_budget_cap.loc[dt_col]

    all_cohort_df['max_capacity'] = max_capacity_val

    all_cohort_df = capacity_constraint(all_cohort_df, 'cohort_capacity', 'max_capacity', dt_col)

    all_cohort_df[dt_col] = all_cohort_df[dt_col]

    all_cohort_df = all_cohort_df.set_index('Service Territory')

    return all_cohort_df[[dt_col]]

def monthly_maint(live_fleet, live_fleet_st_perc, winter_maint_mom, dt_col, maint_creation_df):
    perc_maint_of_live_fleet = maint_creation_df.loc[dt_col]
    ttl_main_vol = live_fleet * perc_maint_of_live_fleet
    st_mnthly_maint_dt = np.outer(live_fleet_st_perc, ttl_main_vol)

    st_mnthly_maint_dt = pd.DataFrame(index=live_fleet_st_perc.index, data=st_mnthly_maint_dt, columns=[dt_col])

    st_mnthly_maint_dt[dt_col] = st_mnthly_maint_dt[dt_col] + winter_maint_mom[dt_col]

    st_mnthly_maint_dt = st_mnthly_maint_dt.applymap(lambda x: x if x%.5!=0 else x+.1)
    st_mnthly_maint_dt = st_mnthly_maint_dt.round(0) # rounds odd unit values up, and even unit values down for .5

    return st_mnthly_maint_dt


###################################################################################

###################################################################################


def remaining_vendor_cap(external_tech_supply_df, vendor_cohort):
    df = pd.concat([vendor_cohort['Dish cohort'], external_tech_supply_df], axis=1)
    cohort_list = list(vendor_cohort['Dish cohort'].unique())
    df_cohort_list = []
    for cohort in cohort_list:
        dish_cohort_cap_val = vendor_cohort[vendor_cohort['Dish cohort']=='a']['capacity'].iloc[0]
        df_res = dish_cohort_cap_val - df[df['Dish cohort']==cohort].sum().iloc[1:]
        df_res.name = 'capacity'
        df_res = df_res.to_frame()
        df_res['cohort'] = cohort
        df_cohort_list.append(df_res.reset_index())
    
    all_cohort_df = pd.concat(df_cohort_list, ignore_index=True, sort=False)
    return all_cohort_df


def format_vendor_cohort_df(external_tech_supply_df, vendor_cohort, dt_col):
    df_mom = remaining_vendor_cap(external_tech_supply_df, vendor_cohort)
    df_mom = df_mom.set_index('index').loc[dt_col]

    vendor_cohort = vendor_cohort.iloc[:,:-1].reset_index()
    df_res = vendor_cohort.merge(df_mom, left_on='Dish cohort', right_on='cohort', how='left').set_index('Service Territory')
    df_res = df_res[list(df_res.columns[:-1])]
    return df_res


###################################################################################

###################################################################################

def get_maint_unconstrained(backlog_date, met_install_df, live_fleet_df, maint_creation_df, winter_maint_mom):
    intial_maint_dt_ser = (live_fleet_df['live_fleet_cnt'] * .1).apply(lambda x: round(x))
    intial_maint_dt_ser.name = backlog_date       

    met_install_df.index.names = ['Service Territory']
    live_fleet_mom_df = live_fleet_df[['live_fleet_cnt']].merge(met_install_df, on='Service Territory', how='left').cumsum(axis=1).iloc[:,1:]
    maint_dt_unconstrained_list = []

    for idx, val in maint_creation_df.items():
        ser = live_fleet_mom_df[idx]*val
        ser = ser.apply(lambda x: math.ceil(x))
        maint_dt_unconstrained_list.append(ser)
        

    maint_dt_unconstrained = pd.concat(maint_dt_unconstrained_list, axis=1)
    maint_dt_unconstrained = maint_dt_unconstrained + winter_maint_mom
    
    maint_dt_unconstrained_w_backlog = pd.concat([intial_maint_dt_ser, maint_dt_unconstrained], axis=1)

    maint_dt_unconstrained_v2 = maint_dt_unconstrained.copy()
    maint_dt_unconstrained_v2['2024-08-01'] = maint_dt_unconstrained_v2['2024-08-01'] + maint_dt_unconstrained_w_backlog['2024-07-01']

    return intial_maint_dt_ser, maint_dt_unconstrained, maint_dt_unconstrained_w_backlog, maint_dt_unconstrained_v2, live_fleet_mom_df