import pandas as pd
import math
import numpy as np

import basic_functions as bf
import input_file_functions as iff
import logic_functions as lf
import logic_prep_functions as lpf

def run_install(main_col_list, install_backlog_ser, initial_local_tech_count, initial_travel_tech_count, install_dt_unconstrained, wo_tech_mnthly_rr_less_ss, 
                max_local_tech_hires, local_tech_hires, dish_vendor_cohort_st, vendor_maint_budget_cap, vendor_install_budget_cap, install_perc_cap_input):

    remaining_install_dt_ser_list = []
    tech_cap_by_st_ser_list = []
    travel_tech_cap_by_st_ser_list = []
    local_tech_install_supply_list = []
    travel_tech_install_supply_list = []
    local_tech_hire_list = []
    as_install_list = []

    internal_tech_needs_list = []
    external_tech_needs_list = []
    internal_tech_supply_list = []
    external_tech_supply_list = []

    net_adds_list = [14, 16, 13, 13, 26, 28]

    # this is only ran once
    remaining_install_dt_ser_list.append(install_backlog_ser) 

    tech_cnt_by_cap_ser_c = initial_local_tech_count.copy()
    tech_cnt_by_cap_ser_c_ = initial_local_tech_count['1P tech count']
    tech_cnt_by_cap_ser_c_.name = main_col_list[0]


    st_travel_tech_cnt_ser = initial_travel_tech_count.copy()

    tech_hiring_itr = 0
    net_adds_list_itr = 0

    for itr in range(0, len(main_col_list)):

        dt_col = main_col_list[itr]

        remaining_install_dt_ser = remaining_install_dt_ser_list[itr]
        remaining_install_dt_ser.fillna(0, inplace=True)

        current_month_install_dt_ser = install_dt_unconstrained.loc[:, dt_col] # only used in last call

        # Need to get new runrate every month
        monthly_tech_rr = wo_tech_mnthly_rr_less_ss.loc[dt_col]

        # if itr !=0 and itr % 3 == 0:
        cur_max_hires_val = max_local_tech_hires[itr]
        if cur_max_hires_val > 0:

            local_tech_hire_ser = local_tech_hire_list[tech_hiring_itr]
            new_tech_cnt_by_cap_ser = lf.update_tech_cnt_by_cap_ser(tech_cnt_by_cap_ser_c, local_tech_hire_ser, monthly_tech_rr, dt_col)
            
            tech_cnt_by_cap_ser_c = new_tech_cnt_by_cap_ser
            tech_cnt_by_cap_ser_c.rename('1P tech count', inplace=True) # new
            tech_cnt_by_cap_ser_c = tech_cnt_by_cap_ser_c.to_frame() # new

            tech_hiring_itr += 1

        st_grounded_travel_tech_cnt_ser = st_travel_tech_cnt_ser.astype(float).applymap(lambda x: math.ceil(x * .5))

        st_ttl_local_tech_cnt_ser = tech_cnt_by_cap_ser_c['1P tech count'] + st_grounded_travel_tech_cnt_ser['Travel tech count']
        st_ttl_travel_tech_cnt_ser = st_travel_tech_cnt_ser['Travel tech count'] - st_grounded_travel_tech_cnt_ser['Travel tech count']

        st_ttl_local_tech_cnt_ser.name = dt_col
        tech_cap_by_st_ser_list.append(st_ttl_local_tech_cnt_ser)

        current_month_local_tech_supply_ser = lf.get_current_month_local_tech_supply(st_ttl_local_tech_cnt_ser, remaining_install_dt_ser, monthly_tech_rr, dt_col, install_perc_cap_input)
        local_tech_install_supply_list.append(current_month_local_tech_supply_ser) 

        cur_mon_excess_tech_needs_ser, _ = lf.techs_needed_to_complete_work(remaining_install_dt_ser, current_month_local_tech_supply_ser, monthly_tech_rr, itr)
        internal_tech_needs_list.append(cur_mon_excess_tech_needs_ser)
        
        current_month_travel_tech_supply_ser = lf.constrain_tech_needs(cur_mon_excess_tech_needs_ser, st_ttl_travel_tech_cnt_ser, monthly_tech_rr, dt_col, install_perc_cap_input)
        travel_tech_install_supply_list.append(current_month_travel_tech_supply_ser)

        surplus_install_dt_ser = lf.get_remaining_install_dt(remaining_install_dt_ser, current_month_local_tech_supply_ser, current_month_travel_tech_supply_ser, current_month_install_dt_ser, dt_col)

        if itr < len(main_col_list)-1:
            next_month_max_hires_val = max_local_tech_hires[itr + 1]

        if next_month_max_hires_val > 0 and itr < len(main_col_list)-1:
            nxt_dt_col = main_col_list[itr + 1]
            st_overwrite_ser = local_tech_hires[local_tech_hires[nxt_dt_col] > 0][nxt_dt_col]

            next_month_max_hires_val = next_month_max_hires_val - st_overwrite_ser.sum()
            new_tech_hiring_ser = lf.get_new_tech_hire_list(surplus_install_dt_ser, next_month_max_hires_val, st_overwrite_ser, monthly_tech_rr)

            new_tech_hiring_ser.name = dt_col
            local_tech_hire_list.append(new_tech_hiring_ser)
            net_adds_list_itr += 1


        current_month_internal_tech_supply_ser = current_month_local_tech_supply_ser + current_month_travel_tech_supply_ser
        internal_tech_supply_list.append(current_month_internal_tech_supply_ser)


        cur_mon_excess_external_tech_needs_ser, _ = lf.techs_needed_to_complete_work(remaining_install_dt_ser, current_month_internal_tech_supply_ser, monthly_tech_rr, itr)
        external_tech_needs_list.append(cur_mon_excess_external_tech_needs_ser)

        current_external_tech_supply_ser = lf.constrain_external_tech_needs(cur_mon_excess_external_tech_needs_ser, dish_vendor_cohort_st,'Dish cohort' , monthly_tech_rr, dt_col, vendor_maint_budget_cap, vendor_install_budget_cap)
        external_tech_supply_list.append(current_external_tech_supply_ser)
        
        surplus_install_dt_ser = lf.get_remaining_install_dt(remaining_install_dt_ser, current_month_internal_tech_supply_ser, current_external_tech_supply_ser, current_month_install_dt_ser, dt_col)
        
        remaining_install_dt_ser_list.append(surplus_install_dt_ser)
        
        as_ser = current_month_internal_tech_supply_ser + current_external_tech_supply_ser[dt_col]

        as_install_list.append(as_ser)

    install_res_dict = {}

    install_res_dict['local_tech_supply'] = pd.concat(local_tech_install_supply_list, axis=1)
    install_res_dict['travel_tech_supply'] = pd.concat(travel_tech_install_supply_list, axis=1)
    install_res_dict['intall_df'] = pd.concat(remaining_install_dt_ser_list, axis=1)
    install_res_dict['qtrly_tech_cap'] = pd.concat(tech_cap_by_st_ser_list, axis=1)
    install_res_dict['met_install_df'] = pd.concat(as_install_list, axis=1)

    install_res_dict['internal_tech_needs_df'] = pd.concat(internal_tech_needs_list, axis=1) # after local supply
    install_res_dict['external_tech_needs_df'] = pd.concat(external_tech_needs_list, axis=1)
    install_res_dict['internal_tech_supply_df'] = pd.concat(internal_tech_supply_list, axis=1)
    install_res_dict['external_tech_supply_df'] = pd.concat(external_tech_supply_list, axis=1)

    return install_res_dict



def run_maintenance(main_col_list, backlog_date, met_install_df, wo_tech_mnthly_rr_less_ss, qtrly_tech_cap, local_tech_supply, travel_tech_supply, external_tech_supply_df \
                    , dish_vendor_cohort_st, nsa_vendor_cohort_st, vendor_maint_budget_cap, vendor_install_budget_cap, live_fleet_df, maint_creation_df, winter_maint_mom):
    
    intial_maint_dt_ser, maint_dt_unconstrained, _ = lf.get_maint_unconstrained(backlog_date, met_install_df, live_fleet_df, maint_creation_df, winter_maint_mom)

    remaining_maint_dt_ser_list = []
    as_maint_list = []

    local_tech_maint_supply_list = []
    travel_tech_maint_supply_list = []
    internal_maint_tech_supply_list = []
    external_maint_tech_supply_list = []
    nsa_external_maint_tech_supply_list = []
    dish_external_maint_tech_supply_list = []

    external_maint_tech_needs_list = []
    external_maint_post_nsa_tech_needs_list = []

    remaining_maint_dt_ser_list.append(intial_maint_dt_ser)

    max_travel_tech_perc = 1  # user input

    for itr in range(0, len(main_col_list)):
        dt_col = main_col_list[itr]

        remaining_maint_dt_ser = remaining_maint_dt_ser_list[itr]

        # Need to get new runrate every month
        monthly_tech_rr = wo_tech_mnthly_rr_less_ss.loc[dt_col]

        tech_cnt_by_cap_ser_c = qtrly_tech_cap[dt_col]

        current_month_local_tech_capacity_ser = (tech_cnt_by_cap_ser_c * monthly_tech_rr) - local_tech_supply.loc[:, dt_col]
        current_month_local_tech_capacity_ser.fillna(0, inplace=True) # append this

        check_df = pd.concat([remaining_maint_dt_ser.fillna(0),current_month_local_tech_capacity_ser], axis=1)
        check_df.fillna(0, inplace=True)
        check_df['ser_b3'] = np.where(check_df.iloc[:, 0] >= check_df.iloc[:, 1],check_df.iloc[:, 1], check_df.iloc[:, 0])
        
        current_month_local_tech_supply_ser = check_df.ser_b3
        current_month_local_tech_supply_ser.rename(dt_col, inplace=True)

        local_tech_maint_supply_list.append(current_month_local_tech_supply_ser)

        travel_tech_rem_monthly_supply_value = travel_tech_supply.loc[:,dt_col].apply(lambda x: math.ceil(x / wo_tech_mnthly_rr_less_ss.loc[dt_col])).sum()

        cur_mon_excess_tech_needs_ser, _ = lf.techs_needed_to_complete_work(remaining_maint_dt_ser, current_month_local_tech_supply_ser, monthly_tech_rr
                                                                        , itr, travel_tech_rem_monthly_supply_value)
        current_month_travel_tech_supply_ser = lf.constrain_tech_needs_maint(cur_mon_excess_tech_needs_ser, travel_tech_rem_monthly_supply_value, monthly_tech_rr, dt_col, itr)
        travel_tech_maint_supply_list.append(current_month_travel_tech_supply_ser)

        current_month_internal_tech_supply_ser = current_month_local_tech_supply_ser + current_month_travel_tech_supply_ser
        internal_maint_tech_supply_list.append(current_month_internal_tech_supply_ser)
        

        # TO-DO: rename variables used in the functions below
        # w384_ser = lf.get_w384(remaining_maint_dt_ser, current_month_local_tech_supply_ser, current_month_travel_tech_supply_ser,itr)
        # b384_ser = lf.get_b384(w384_ser, itr, dt_col)
        
        # Vendor stuff below
        remaining_monthly_maint_budget_val = 0
        cur_mon_excess_external_tech_needs_ser, _ = lf.techs_needed_to_complete_work(remaining_maint_dt_ser, current_month_internal_tech_supply_ser, monthly_tech_rr, itr)
        external_maint_tech_needs_list.append(cur_mon_excess_external_tech_needs_ser)

        dish_date_vendor_cohort_st = lf.format_vendor_cohort_df(external_tech_supply_df, dish_vendor_cohort_st, dt_col)

        nsa_current_external_maint_tech_supply_ser = lf.constrain_external_tech_needs(cur_mon_excess_external_tech_needs_ser, nsa_vendor_cohort_st,'NSA cohort' 
                                                                                , monthly_tech_rr, dt_col, vendor_maint_budget_cap, vendor_install_budget_cap, remaining_monthly_maint_budget_val)
        nsa_external_maint_tech_supply_list.append(nsa_current_external_maint_tech_supply_ser)

        # Update value
        remaining_monthly_maint_budget_val = nsa_current_external_maint_tech_supply_ser[dt_col].sum()

        # Apply nsa supply to remaing dt
        current_int_nsa_maint_tech_supply_ser = current_month_internal_tech_supply_ser + nsa_current_external_maint_tech_supply_ser[dt_col]
        cur_mon_excess_post_nsa_tech_needs_ser, _ = lf.techs_needed_to_complete_work(remaining_maint_dt_ser, current_int_nsa_maint_tech_supply_ser, monthly_tech_rr, itr)
        external_maint_post_nsa_tech_needs_list.append(cur_mon_excess_post_nsa_tech_needs_ser)

        dish_current_external_maint_tech_supply_ser = lf.constrain_external_tech_needs(cur_mon_excess_post_nsa_tech_needs_ser, dish_date_vendor_cohort_st,'Dish cohort' 
                                                                                    , monthly_tech_rr, dt_col, vendor_maint_budget_cap, vendor_install_budget_cap, remaining_monthly_maint_budget_val)
        dish_external_maint_tech_supply_list.append(dish_current_external_maint_tech_supply_ser)

        # Combine external supplies
        current_external_maint_tech_supply_ser = nsa_current_external_maint_tech_supply_ser + dish_current_external_maint_tech_supply_ser
        external_maint_tech_supply_list.append(current_external_maint_tech_supply_ser)

        surplus_maint_dt_ser = remaining_maint_dt_ser - (current_month_internal_tech_supply_ser + current_external_maint_tech_supply_ser[dt_col]) + maint_dt_unconstrained.iloc[:,itr]
        surplus_maint_dt_ser.rename(dt_col, inplace=True)
        remaining_maint_dt_ser_list.append(surplus_maint_dt_ser)    

        if itr==0:
            as_ser = current_month_local_tech_supply_ser + current_month_travel_tech_supply_ser + current_external_maint_tech_supply_ser[dt_col]
        else:
            as_ser = current_month_local_tech_supply_ser + current_month_travel_tech_supply_ser + current_external_maint_tech_supply_ser[dt_col] + as_maint_list[itr-1]
        
        as_ser.rename(dt_col, inplace=True)
        as_maint_list.append(as_ser)

    maint_res_dict = {}

    maint_res_dict['local_tech_maint_supply'] = pd.concat(local_tech_maint_supply_list, axis=1)
    maint_res_dict['travel_tech_maint_supply'] = pd.concat(travel_tech_maint_supply_list, axis=1)
    maint_res_dict['maint_df'] = pd.concat(remaining_maint_dt_ser_list, axis=1)
    maint_res_dict['met_maint_df'] = pd.concat(as_maint_list, axis=1)

    maint_res_dict['internal_maint_tech_supply_df'] = pd.concat(internal_maint_tech_supply_list, axis=1)
    maint_res_dict['external_maint_tech_supply_df'] = pd.concat(external_maint_tech_supply_list, axis=1)
    maint_res_dict['nsa_external_maint_tech_supply_df'] = pd.concat(nsa_external_maint_tech_supply_list, axis=1)
    maint_res_dict['dish_external_maint_tech_supply_df'] = pd.concat(dish_external_maint_tech_supply_list, axis=1)
    maint_res_dict['external_maint_tech_needs_df'] = pd.concat(external_maint_tech_needs_list, axis=1)
    maint_res_dict['external_maint_post_nsa_tech_needs_df'] = pd.concat(external_maint_post_nsa_tech_needs_list, axis=1)

    return maint_res_dict

def get_main_st(input_data):
    vendor_cohort_st = pd.read_excel(input_data, sheet_name='vendor inputs', header=23, usecols="A:C")
    vendor_cohort_st = vendor_cohort_st.set_index('Service Territory')
    st_index = vendor_cohort_st.index
    st_df = pd.DataFrame(index=st_index)

    return st_df

def run_model(input_data):
    backlog_date = '2024-07-01'

    st_df = get_main_st(input_data)

    flight_tech_df = iff.get_single_inputs(input_data)
    live_fleet_df = iff.get_live_fleet_data(input_data, st_df)
    monthly_inputs, winter_maint_mom = iff.get_monthly_data(input_data, st_df)
    main_col_list = bf.get_main_col_list(monthly_inputs)

    install_perc_cap_input = lpf.get_install_perc_cap_input(monthly_inputs)
    maint_creation_df = lpf.get_maint_creation(monthly_inputs)

    install_backlog_df, locs_in_implementation_df = iff.get_install_implementation_data(input_data)
    sales_funnel_sla_df, new_sales_sla, sales_distb = iff.get_sales_data(input_data, st_df)

    main_col_list = bf.get_main_col_list(monthly_inputs)
    install_backlog_ser, install_dt_unconstrained, install_dt_unconstrained_w_backlog= lpf.get_install_unconstrained(main_col_list, sales_funnel_sla_df, sales_distb, install_backlog_df, locs_in_implementation_df, st_df)
    initial_local_tech_count, initial_travel_tech_count = iff.get_initial_tech_counts(input_data, st_df)

    wo_tech_mnthly_rr_less_ss = lpf.get_wo_tech_mnthly_rr_less_ss(flight_tech_df, monthly_inputs)
    max_local_tech_hires, local_tech_hires = iff.get_local_tech_hires(input_data, st_df)

    vendor_capacity, nsa_vendor_cohort_st, dish_vendor_cohort_st = iff.get_vendor_data(input_data)
    vendor_install_budget_cap = bf.convert_date_idx_to_str(vendor_capacity.loc['3P install max (budget)'])
    vendor_maint_budget_cap = bf.convert_date_idx_to_str(vendor_capacity.loc['3P maint max (budget)'])

    install_res_dict = \
            run_install(main_col_list, install_backlog_ser, initial_local_tech_count, initial_travel_tech_count, install_dt_unconstrained, wo_tech_mnthly_rr_less_ss, 
                max_local_tech_hires, local_tech_hires, dish_vendor_cohort_st, vendor_maint_budget_cap, vendor_install_budget_cap, install_perc_cap_input)
    
    maint_res_dict = \
            run_maintenance(main_col_list, backlog_date, install_res_dict['met_install_df'], wo_tech_mnthly_rr_less_ss, install_res_dict['qtrly_tech_cap'], install_res_dict['local_tech_supply'], install_res_dict['travel_tech_supply'], \
                            install_res_dict['external_tech_supply_df'], dish_vendor_cohort_st, nsa_vendor_cohort_st, vendor_maint_budget_cap, vendor_install_budget_cap, live_fleet_df, maint_creation_df, winter_maint_mom)

    install_res_dict['install_dt_unconstrained'] = install_dt_unconstrained
    install_res_dict['install_dt_unconstrained_w_backlog'] = install_dt_unconstrained_w_backlog

    return install_res_dict, maint_res_dict, wo_tech_mnthly_rr_less_ss