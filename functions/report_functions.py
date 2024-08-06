import numpy as np

def get_cap_perc_ma3(install_res_dict, maint_res_dict, wo_tech_mnthly_rr_less_ss):
    install_dt_unconstrained = install_res_dict['install_dt_unconstrained']
    maint_dt_unconstrained = maint_res_dict['maint_dt_unconstrained']
    qtrly_tech_cap = install_res_dict['qtrly_tech_cap']

    roll_num = 3
    rolling_supply = (qtrly_tech_cap.rolling(3, axis=1).mean() * wo_tech_mnthly_rr_less_ss.rolling(3, axis=0).mean())

    ttl_demand_df = install_dt_unconstrained + maint_dt_unconstrained
    cap_perc_ma3 = (ttl_demand_df.rolling(roll_num, axis=1).mean()-rolling_supply) / (rolling_supply)
    cap_perc_ma3 = cap_perc_ma3.iloc[:, roll_num-1:]
    cap_perc_ma3 = cap_perc_ma3.replace([np.inf, -np.inf], 0).fillna(0)
    return cap_perc_ma3, ttl_demand_df

def get_cap_perc(install_res_dict, maint_res_dict, wo_tech_mnthly_rr_less_ss):
    install_dt_unconstrained = install_res_dict['install_dt_unconstrained']
    maint_dt_unconstrained = maint_res_dict['maint_dt_unconstrained']
    qtrly_tech_cap = install_res_dict['qtrly_tech_cap']

    supply_df = (qtrly_tech_cap * wo_tech_mnthly_rr_less_ss)
    ttl_demand_df = install_dt_unconstrained + maint_dt_unconstrained

    cap_perc = ttl_demand_df / supply_df
    cap_perc = cap_perc.replace([np.inf, -np.inf], 0).fillna(0)
    
    return cap_perc