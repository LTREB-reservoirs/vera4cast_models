import os
import torch
from src.helper_utils import (load_config, get_model_id, chla_lag_days, chla_lag_source, met_driver_lag_days,
                              training_data_dir, check_no_overwrite, LAST_CHLA_VAR)
from src.pull_usgsrc4cast_data import pull_target_data
from src.pull_predicted_chl import pull_predicted_chl
from src.pull_vera4cast_chla_data import build_chla_lag, chla_lag_url
from src.pull_static_data import pull_static_features
from src.metadata_utils import get_metadata
from src.prep_data_utils import *


def data_prep(config_file, root_dir=None):
    """
    Prepares the dataset for training, validation, and testing of a model by reading, merging,
    and processing water temperature, reservoir releases, and gridmet data.

    This function performs the following steps:
    1. Reads input CSV files for water temperature, reservoir releases, and gridmet data.
    2. Processes and merges these datasets based on a common date index.
    3. Creates lagged features for the specified target variable if applicable.
    4. Splits the dataset into training, validation, and test sets.
    5. Scales the features using z-score normalization.
    6. Reshapes the data for model input.
    7. Saves the processed data to a compressed NumPy file.

    config:
        config (dict): A dictionary containing configuration parameters, which may include:
            - 'reservoir_data_file' (str): Path to the CSV file containing reservoir release data.
            - 'water_temp_data_file' (str): Path to the CSV file containing water temperature data.
            - 'gridmet_data_file' (str): Path to the CSV file containing GridMET data.
            - 'min_date' (str): Minimum date for the dataset (format: 'YYYY-MM-DD').
            - 'max_date' (str): Maximum date for the dataset (format: 'YYYY-MM-DD').
            - 'time_idx_name' (str): Name of the time index variable in the dataset.
            - 'spatial_idx_name' (str): Name of the spatial index variable in the dataset.
            - 'y_vars' (list): List of target variable names for the model.
            - 'x_vars' (list): List of predictor variable names for the model.
            - 'lag_days' (int): Number of lag days to apply for the target variable.
            - Training data is saved to in/training_data/{model_id}.npz (derived from config settings).
            - 'start_date_train' (str): Start date for the training set.
            - 'end_date_train' (str): End date for the training set.
            - 'start_date_val' (str): Start date for the validation set.
            - 'end_date_val' (str): End date for the validation set.
            - 'start_date_test' (str): Start date for the test set.
            - 'end_date_test' (str): End date for the test set.
            - 'hidden_units' (int): Number of hidden units for the model.

    Returns:
        dict: A dictionary containing processed data, including:
            - 'x_train': Training set features.
            - 'x_val': Validation set features.
            - 'x_test': Test set features.
            - 'x_all_dates': Features for all dates in the dataset.
            - 'x_mean': Mean values of the features.
            - 'x_std': Standard deviation of the features.
            - 'obs_train': Training set observations.
            - 'obs_val': Validation set observations.
            - 'obs_test': Test set observations.
            - 'obs_all_dates': Observations for all dates in the dataset.
            - 'pretrain_train': Pre-training set features.
            - 'pretrain_val': Pre-training set validation features.
            - 'pretrain_test': Pre-training set test features.
            - 'lag_var': Name of the lagged variable.
            - 'lag_var_source': Name of the lagged water temperature source variable.
            - 'lag_var_mean': Mean of the lagged variable.
            - 'lag_var_std': Standard deviation of the lagged variable.
            - Other relevant data structures for model training.

    """
    config = load_config(config_file)

    site_metadata_xr = get_metadata(config['site_metadata_url'],
                                    config['savoy_metadata_file'],
                                    config['stackpoole_metadata_file'],
                                    sites = config['sites_to_include'])

    start_time = np.datetime64(config['min_date'])
    end_time = np.datetime64(config['max_date'])

    # Read in input files for observations and driver data
    obs_xr = xr.load_dataset(
        filename_or_obj=config['obs_local_file'],
        engine="netcdf4",
        chunks = None)

    # weather drivers pull locally
    gefs_xr = xr.load_dataset(
        filename_or_obj=config['gefs_local_file'],
        engine="netcdf4",
        chunks = None)
    gefs_xr = aggregate_analysis_gefs(
        ds=gefs_xr,
        out_vars=config['x_vars']
    )

    gefs_operational_xr = xr.load_dataset(
        filename_or_obj=config['gefs_operational_local_file'],
        engine="netcdf4",
        chunks=None,
        decode_timedelta=True
    )
    # aggregating to daily scale
    gefs_operational_xr = (
        aggregate_operational_gefs(
            ds=gefs_operational_xr,
            out_vars=config['x_vars'])
        .rename({"init_time": "time"})
    )
    # rename init_time to time for merging other datasets

    # Calculate forecast uncertainty (PI90) from GEFS ensemble if enabled
    if config.get('include_forecast_uncertainty', False):
        print("Calculating forecast uncertainty (PI90) from GEFS ensemble...")

        # Calculate PI90 from operational GEFS ensemble
        gefs_uncertainty_xr = calculate_forecast_uncertainty(
            ds=gefs_operational_xr,
            met_vars=config['x_vars'],
            include_median=False  # We already have the values, just need PI90
        )

        # Merge PI90 into operational dataset
        gefs_operational_xr = xr.merge([gefs_operational_xr, gefs_uncertainty_xr])

        # Calculate climatology with uncertainty from historical GEFS analysis
        print("Calculating climatology with uncertainty for fallback...")
        climatology_xr = calculate_climatology_with_uncertainty(
            ds_historical=gefs_xr,
            met_vars=config['x_vars'],
            pi90_multiplier=config.get('climatology_pi90_multiplier', 4.0)
        )

        # Store climatology for potential use in training data
        # (for dates without operational GEFS forecasts)
        config['_climatology_xr'] = climatology_xr

    # hydro drivers pull locally
    hydro_xr = xr.load_dataset(
        filename_or_obj=config['hydro_local_file'],
        engine="netcdf4",
        chunks = None)

    # static features
    static_xr = pull_static_features(
        site_metadata=site_metadata_xr,
        variables=config['x_vars_static']
    )

    # xarray with all data, combined by time and site_id
    data_xr = (
        xr.combine_by_coords([gefs_xr,
                              obs_xr,
                              hydro_xr,
                              static_xr],
                              join ='left')
        .where(lambda x: x.source_dataset.isin(config['sites_to_include']), drop = True)
    )
    if config.get('site_ids_to_include'):
        data_xr = data_xr.where(data_xr.site_id.isin(config['site_ids_to_include']), drop=True)
    # expanding out the static features to all time points
    data_xr[config['x_vars_static']] = data_xr[config['x_vars_static']].broadcast_like(data_xr.time)

    # Add PI90 features to training data using climatology (high uncertainty fallback)
    # This teaches the model that large PI90 = unreliable forecast
    if config.get('include_forecast_uncertainty', False):
        climatology_xr = config['_climatology_xr']
        base_met_vars = [v for v in config['x_vars'] if not v.endswith('_pi90')]
        # Map climatology by day of year to training data dates
        for var in base_met_vars:
            pi90_var = f"{var}_pi90"
            # Create a DataArray with climatology PI90 aligned to training data
            # by mapping each date to its day-of-year climatology value
            clim_pi90_aligned = climatology_xr[pi90_var].sel(
                dayofyear=data_xr['time'].dt.dayofyear
            )
            # Broadcast across sites
            data_xr[pi90_var] = clim_pi90_aligned.broadcast_like(data_xr.time)

    data_operational_xr = (
        xr.combine_by_coords([gefs_operational_xr,
                              obs_xr,
                              hydro_xr,
                              static_xr],
                              join='left')
        .where(lambda x: x.source_dataset.isin(config['sites_to_include']), drop = True)
    )
    if config.get('site_ids_to_include'):
        data_operational_xr = data_operational_xr.where(data_operational_xr.site_id.isin(config['site_ids_to_include']), drop=True)
    # expand out static features
    data_operational_xr[config['x_vars_static']] = data_operational_xr[config['x_vars_static']].broadcast_like(data_operational_xr[['time','lead_time','ensemble_member']])

    # PI90 variables are already properly aligned with time/lead_time/site_id from GEFS
    # but need to be broadcast across ensemble_member dimension (since PI90 is computed from ensemble)
    if config.get('include_forecast_uncertainty', False):
        base_met_vars = [v for v in config['x_vars'] if v in gefs_operational_xr.data_vars and not v.endswith('_pi90')]
        pi90_vars_in_data = [f"{var}_pi90" for var in base_met_vars if f"{var}_pi90" in data_operational_xr.data_vars]
        if pi90_vars_in_data:
            data_operational_xr[pi90_vars_in_data] = data_operational_xr[pi90_vars_in_data].broadcast_like(data_operational_xr[['ensemble_member']])
    # not broadcasting out chla or discharge because we don't have forecasted values
    if not config['hydro_vars']:
        data_operational_xr[config['y_vars']+['river_discharge']] = data_operational_xr[config['y_vars']+['river_discharge']].broadcast_like(data_operational_xr[['ensemble_member']])
    else:
        data_operational_xr[config['y_vars']+config['hydro_vars']] = data_operational_xr[config['y_vars']+config['hydro_vars']].broadcast_like(data_operational_xr[['ensemble_member']])

    # keep NEON sites? they kinda suck
    # if config['remove_neon_sites']:
    #     data_xr = data_xr.drop_sel(site_id=[site_id for site_id in data_xr.site_id.values if site_id.startswith("NEON-")])

    # Build x_vars list with met, hydro, static, and optionally PI90 features
    base_met_vars = config['x_vars'].copy()  # Save original met vars
    if not config['hydro_vars']:
        config['x_vars'] = config['x_vars'] + config['x_vars_static']
    else:
        config['x_vars'] = config['x_vars'] + config['hydro_vars'] + config['x_vars_static']

    # Add PI90 variables to x_vars if forecast uncertainty is enabled
    if config.get('include_forecast_uncertainty', False):
        pi90_vars = [f"{var}_pi90" for var in base_met_vars]
        config['x_vars'] = config['x_vars'] + pi90_vars
        print(f"Added {len(pi90_vars)} PI90 variables to features: {pi90_vars}")

    # lag the chl and source input feature by n lag days if using as predictor variable
    lag_days = config.get('lag_days', 1)
    lag_source = chla_lag_source(config)
    if lag_source == 'realtime':
        # Real-time observed chla (VERA4cast Chla_ugL_mean) replaces the
        # obs + noAR-prediction combo used by lag_target.
        rt_lag_days = chla_lag_days(config)
        lagged_xr = build_chla_lag(
            url=chla_lag_url(config),
            start_time=start_time,
            end_time=end_time,
            lag_days=rt_lag_days,
            site_ids=config.get('site_ids_to_include'),
        ).reindex(time=data_xr.time)
        # Operational side is indexed by init_time: the lag is a day-0 input, so
        # broadcast the (time, site_id) series across the lead_time/ensemble dims.
        lagged_operational_xr = build_chla_lag(
            url=chla_lag_url(config),
            start_time=data_operational_xr.time.min().values,
            end_time=data_operational_xr.time.max().values,
            lag_days=rt_lag_days,
            site_ids=config.get('site_ids_to_include'),
        ).reindex(time=data_operational_xr.time).broadcast_like(
            data_operational_xr[[config['x_vars'][0]]]
        )
        data_xr = (
            xr.merge([data_xr, lagged_xr], join='left')
            .isel(time=slice(rt_lag_days, None))
        )
        data_operational_xr = (
            xr.merge([data_operational_xr, lagged_operational_xr], join='left')
            .isel(time=slice(rt_lag_days, None))
        )
        config['x_vars'] = config['x_vars'] + ['chla_lagged', 'chla_uncertainty_lagged']
    elif lag_source == 'noAR':
        # shifting the xarray dataset shifts all the data so need to create another xarray with
        #  just the variable we want lagged
        pred_chl_xr = pull_predicted_chl(
            data_file=config['lstm_noAR_local_file'],
            start_time=start_time,
            end_time=end_time
        )
        obs_chl_xr = (
            data_xr['chla']
            .to_dataset()
            # Observation uncertainty: constant 5% CV
            # SD = 5% of measurement, PI90 = 3.29 × SD
            .assign(chla_uncertainty = lambda x: 3.29 * 0.05 * np.abs(x.chla))
        )
        obs_chl_operational_xr = (
            data_operational_xr['chla']
            .to_dataset()
            # Observation uncertainty: constant 5% CV
            # SD = 5% of measurement, PI90 = 3.29 × SD
            .assign(chla_uncertainty = lambda x: 3.29 * 0.05 * np.abs(x.chla))
        )
        # Reindex pred_chl to match obs_chl coordinates for proper combine_first
        # This ensures site_id order and time values align exactly
        pred_chl_aligned = pred_chl_xr.reindex_like(obs_chl_xr, method=None)
        pred_chl_operational_aligned = pred_chl_xr.reindex_like(obs_chl_operational_xr, method=None)
        # combine with observed chla with predicted chla
        # first dataset (obs_chl) takes precedence over the combined dataset (pred_chl)
        #  all other points are filled with second dataset (predicted chlorophyll)
        obs_pred_chl_xr = obs_chl_xr.combine_first(pred_chl_aligned)
        obs_pred_chl_operational_xr = (
            obs_chl_operational_xr.combine_first(pred_chl_operational_aligned)
            .sel(time = slice(obs_chl_operational_xr.time.min().values,
                              obs_chl_operational_xr.time.max().values))
        )

        lagged_xr = (
            obs_pred_chl_xr[['chla', 'chla_uncertainty']]
            .shift(time=config['lag_days'])
            .rename({"chla": "chla_lagged",
                    "chla_uncertainty": "chla_uncertainty_lagged"})
        )
        lagged_operational_xr = (
            obs_pred_chl_operational_xr[['chla', 'chla_uncertainty']]
            .shift(time=config['lag_days'])
            .rename({"chla": "chla_lagged",
                    "chla_uncertainty": "chla_uncertainty_lagged"})
        )
        data_xr = (
            xr.combine_by_coords([data_xr, lagged_xr], join='left')
            # slice the dataset to get rid of new NA's created by lagging the autoregresive variable
            .isel(time = slice(config['lag_days'], None))
        )
        data_operational_xr = (
            xr.combine_by_coords([data_operational_xr,
                                  lagged_operational_xr],
                                  join='left')
            # slice the dataset to get rid of new NA's created by lagging the autoregresive variable
            .isel(time = slice(config['lag_days'], None))
        )
        config['x_vars'] = config['x_vars'] + ['chla_lagged', 'chla_uncertainty_lagged']

    x_xr = data_xr[config['x_vars']]
    obs_xr = data_xr[config['y_vars']]
    pretrain_xr = data_xr[config['y_vars']]

    # Log-transform target variable if configured (matches encoder-decoder behavior)
    # Applies log(chla + 0.01) to handle zeros; predictions back-transformed via exp(q) - 0.01
    log_transform_target = config.get('log_transform_target', False)
    if log_transform_target:
        print("Log-transforming target variable (chlorophyll): log(chla + 0.01)")
        obs_xr = np.log(obs_xr + 0.01)
        pretrain_xr = np.log(pretrain_xr + 0.01)

    x_forecast_xr = data_operational_xr[config['x_vars']]

    # scale, etc...
    x_train, x_val, x_test = separate_trn_tst(x_xr,
                                              config['time_idx_name'],
                                              config['start_date_train'],
                                              config['end_date_train'],
                                              config['start_date_val'],
                                              config['end_date_val'],
                                              config['start_date_test'],
                                              config['end_date_test'],
                                              config['spatial_idx_name'])

    # x_data used for predicting across all times
    x_all_dates, _, _ = separate_trn_tst(x_xr,
                                        config['time_idx_name'],
                                        config['min_date'], # min and max of dataset
                                        config['max_date'],
                                        config['start_date_val'],
                                        config['end_date_val'],
                                        config['start_date_test'],
                                        config['end_date_test'],
                                        config['spatial_idx_name'])

    pretrain_train, pretrain_val, pretrain_test = separate_trn_tst(pretrain_xr,
                                                                config['time_idx_name'],
                                                                config['start_date_train'],
                                                                config['end_date_train'],
                                                                config['start_date_val'],
                                                                config['end_date_val'],
                                                                config['start_date_test'],
                                                                config['end_date_test'],
                                                                config['spatial_idx_name'],
                                                                config['y_vars'])

    obs_train, obs_val, obs_test = separate_trn_tst(obs_xr,
                                                    config['time_idx_name'],
                                                    config['start_date_train'],
                                                    config['end_date_train'],
                                                    config['start_date_val'],
                                                    config['end_date_val'],
                                                    config['start_date_test'],
                                                    config['end_date_test'],
                                                    config['spatial_idx_name'],
                                                    config['y_vars'])

    obs_all_dates, _, _ = separate_trn_tst(obs_xr,
                                            config['time_idx_name'],
                                            config['min_date'], # min and max of dataset
                                            config['max_date'],
                                            config['start_date_val'],
                                            config['end_date_val'],
                                            config['start_date_test'],
                                            config['end_date_test'],
                                            config['spatial_idx_name'])

    # z-scoring input features
    x_train_scl, x_std, x_mean = scale(x_train)

    # z-scoring if val and test are available, use standard deviation and mean from x training set for scaling
    if x_val:
        x_val_scl, _, _ = scale(x_val, std=x_std, mean=x_mean)
    else:
        x_val_scl = None

    if x_test:
        x_test_scl, _, _ = scale(x_test, std=x_std, mean=x_mean)
    else:
        x_test_scl = None

    if x_all_dates:
        x_all_dates_scl, _, _ = scale(x_all_dates, std=x_std, mean=x_mean)
    else:
        x_all_dates_scl = None

    if config['lag_target']:
        lag_var_mean = x_mean['chla_lagged'].values
        lag_var_std = x_std['chla_lagged'].values
        lag_var_uncertainty_mean = x_mean['chla_uncertainty_lagged'].values
        lag_var_uncertainty_std = x_std['chla_uncertainty_lagged'].values
    else:
        lag_var_mean = np.nan
        lag_var_std = np.nan
        lag_var_uncertainty_mean = np.nan
        lag_var_uncertainty_std = np.nan

    x_data_dict = {
        "x_train": convert_batch_reshape(x_train_scl,
                                        config['spatial_idx_name'],
                                        config['time_idx_name']),
        "x_val": convert_batch_reshape(x_val_scl,
                                       config['spatial_idx_name'],
                                       config['time_idx_name']),
        "x_test": convert_batch_reshape(x_test_scl,
                                        config['spatial_idx_name'],
                                        config['time_idx_name']),
        "x_all_dates": convert_batch_reshape(x_all_dates_scl,
                                        config['spatial_idx_name'],
                                        config['time_idx_name'],
                                        seq_len=len(x_all_dates_scl['time']),
                                        fill_batch=False),
        "x_std": x_std.to_array().values,
        "x_mean": x_mean.to_array().values,
        "x_vars": np.array(config['x_vars']),
        "ids_train": coord_as_reshaped_array(
            x_train_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name']),
        "times_train": coord_as_reshaped_array(
            x_train_scl,
            config['time_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name'],
            fill_time = True),
        "padded_train": coord_as_reshaped_array(
            x_train_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name'],
            fill_pad=True),
        "ids_val": coord_as_reshaped_array(
            x_val_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name']),
        "times_val": coord_as_reshaped_array(
            x_val_scl,
            config['time_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name']),
        "padded_val": coord_as_reshaped_array(
            x_val_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name'],
            fill_pad=True),
        "ids_test": coord_as_reshaped_array(
            x_test_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name']),
        "times_test": coord_as_reshaped_array(
            x_test_scl,
            config['time_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name']),
        "padded_test": coord_as_reshaped_array(
            x_test_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name'],
            fill_pad=True),
        "ids_all_dates": coord_as_reshaped_array(
            x_all_dates_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name'],
            seq_len=len(x_all_dates_scl['time']),
            fill_batch=False),
        "times_all_dates": coord_as_reshaped_array(
            x_all_dates_scl,
            config['time_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name'],
            seq_len=len(x_all_dates_scl['time']),
            fill_batch=False),
        "padded_all_dates": coord_as_reshaped_array(
            x_all_dates_scl,
            config['spatial_idx_name'],
            config['spatial_idx_name'],
            config['time_idx_name'],
            seq_len=len(x_all_dates_scl['time']),
            fill_batch=False,
            fill_pad=True),
        "lag_var": 'chla_lagged',
        "lag_var_uncertainty": 'chla_uncertainty_lagged',
        "lag_var_pos": np.where(config['x_vars'] == np.atleast_1d('chla_lagged'))[0],
        "lag_var_uncertainty_pos": np.where(config['x_vars'] == np.atleast_1d('chla_uncertainty_lagged'))[0],
        "lag_var_mean": lag_var_mean,
        "lag_var_std": lag_var_std,
        "lag_var_uncertainty_mean": lag_var_uncertainty_mean,
        "lag_var_uncertainty_std": lag_var_uncertainty_std
    }

    states_dict = {
        "h_train": torch.zeros(x_data_dict["x_train"].shape[0], config['hidden_units']),
        "c_train": torch.zeros(x_data_dict["x_train"].shape[0], config['hidden_units']),
        "h_val": torch.zeros(x_data_dict["x_val"].shape[0], config['hidden_units']),
        "c_val": torch.zeros(x_data_dict["x_val"].shape[0], config['hidden_units']),
        "h_test": torch.zeros(x_data_dict["x_test"].shape[0], config['hidden_units']),
        "c_test": torch.zeros(x_data_dict["x_test"].shape[0], config['hidden_units']),
        "h_all_dates": torch.zeros(x_data_dict["x_all_dates"].shape[0], config['hidden_units']),
        "c_all_dates": torch.zeros(x_data_dict["x_all_dates"].shape[0], config['hidden_units'])
    }

    weighting_matrix_dict = {
        # fake weighting matrix to make work with our LSTM
        "weighting_matrix_train": np.array(0, ndmin=2),
        "weighting_matrix_val": np.array(0, ndmin=2),
        "weighting_matrix_test": np.array(0, ndmin=2),
        "weighting_matrix_all_dates": np.array(0, ndmin=2)
    }

    pretrain_train_scl, pretrain_std, pretrain_mean = scale(pretrain_train)

    if pretrain_val:
        pretrain_val_scl, _, _ = scale(pretrain_val, std=pretrain_std, mean=pretrain_mean)
    else:
        pretrain_val_scl = None

    if pretrain_test:
        pretrain_test_scl, _, _ = scale(pretrain_test, std=pretrain_std, mean=pretrain_mean)
    else:
        pretrain_test_scl = None

    pretrain_data_dict = {
        "pretrain_train": convert_batch_reshape(pretrain_train,
                                            config['spatial_idx_name'],
                                            config['time_idx_name']),
        "pretrain_val": convert_batch_reshape(pretrain_val,
                                            config['spatial_idx_name'],
                                            config['time_idx_name']),
        "pretrain_test": convert_batch_reshape(pretrain_test,
                                             config['spatial_idx_name'],
                                             config['time_idx_name']),
        "pretrain_std": pretrain_std.to_array().values,
        "pretrain_mean": pretrain_mean.to_array().values,
        "pretrain_obs_vars": config['y_vars']
    }

    obs_train_scl, obs_std, obs_mean = scale(obs_train)

    if obs_val:
        obs_val_scl, _, _ = scale(obs_val, std=obs_std, mean=obs_mean)
    else:
        obs_val_scl = None

    if obs_test:
        obs_test_scl, _, _ = scale(obs_test, std=obs_std, mean=obs_mean)
    else:
        obs_test_scl = None

    if obs_all_dates:
        obs_all_dates_scl, _, _ = scale(obs_all_dates, std=obs_std, mean=obs_mean)
    else:
        obs_all_dates_scl = None

    obs_data_dict = {
        "obs_train": convert_batch_reshape(obs_train,
                                            config['spatial_idx_name'],
                                            config['time_idx_name']),
        "obs_val": convert_batch_reshape(obs_val,
                                            config['spatial_idx_name'],
                                            config['time_idx_name']),
        "obs_test": convert_batch_reshape(obs_test,
                                             config['spatial_idx_name'],
                                             config['time_idx_name']),
        "obs_all_dates": convert_batch_reshape(obs_all_dates,
                                        config['spatial_idx_name'],
                                        config['time_idx_name'],
                                        seq_len=len(x_all_dates_scl['time']),
                                        fill_batch=False),
        "obs_std": obs_std.to_array().values,
        "obs_mean": obs_mean.to_array().values,
        "obs_vars": config['y_vars'],
        "target_log_transformed": log_transform_target
    }

    x_forecast_xr_scl, _, _ = scale(x_forecast_xr, std=x_std, mean=x_mean)

    all_data = {**x_data_dict, **pretrain_data_dict, **obs_data_dict, **states_dict, **weighting_matrix_dict}

    # reorder all dimensions in xarray so they're in order of Time, Latitude, Longitude, Uncertainty
    x_forecast_xr_scl = x_forecast_xr_scl.transpose("time", "lead_time", "site_id", "ensemble_member")
    # expanding out river discharge, lagged chla, and lagged chla uncertainty
    if not config['hydro_vars']:
        print('no hydro variables')
    else:
        new_river_discharge = np.full(x_forecast_xr_scl['temperature_2m'].shape, np.nan)
        new_river_discharge[:,0,:,:] = x_forecast_xr_scl['river_discharge'].values
        x_forecast_xr_scl['river_discharge'] = (('time', 'lead_time', 'site_id', 'ensemble_member'), new_river_discharge)
    if config['lag_target']:
        new_chla_lagged = np.full(x_forecast_xr_scl['temperature_2m'].shape, np.nan)
        new_chla_lagged[:,0,:,:] = x_forecast_xr_scl['chla_lagged'].values
        x_forecast_xr_scl['chla_lagged'] = (('time', 'lead_time', 'site_id', 'ensemble_member'), new_chla_lagged)
        new_chla_uncertainty_lagged = np.full(x_forecast_xr_scl['temperature_2m'].shape, np.nan)
        new_chla_uncertainty_lagged[:,0,:,:] = x_forecast_xr_scl['chla_uncertainty_lagged'].values
        x_forecast_xr_scl['chla_uncertainty_lagged'] = (('time', 'lead_time', 'site_id', 'ensemble_member'), new_chla_uncertainty_lagged)
    else:
        print('no lagged variables')


    x_forecast_xr_scl.to_netcdf(path = config['forecast_data_file'],
                                engine ='netcdf4', mode = 'w')

    model_id = get_model_id(config)
    output_file = os.path.join(training_data_dir(config), f'{model_id}.npz')
    check_no_overwrite([output_file], config)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    print(f"Saving training data to {output_file}")
    np.savez_compressed(output_file, **all_data)

    return all_data

def aggregate_operational_gefs(
        ds,
        out_vars
):
    """
    aggregating operational GEFS to daily predictions
    """
    mean_vars = ["downward_long_wave_radiation_flux_surface", "downward_short_wave_radiation_flux_surface", "temperature_2m", "precipitation_surface", "total_cloud_cover_atmosphere", "wind_u_10m", "wind_v_10m"]
    max_vars = ["maximum_temperature_2m"]
    min_vars = ["minimum_temperature_2m"]
    ds_mean = ds[mean_vars].resample(lead_time='1d').mean()
    ds_max = ds[max_vars].resample(lead_time='1d').max()
    ds_min = ds[min_vars].resample(lead_time='1d').min()

    out_ds = (
        xr.merge([ds_mean, ds_max, ds_min])[out_vars]
    )

    return out_ds

def load_operational_gefs_daily(path, variables, site_ids=None):
    """Load an hourly operational GEFS file and aggregate it to daily lead times.

    The stage2 training file is large (init_time x hourly lead_time x 31 members x
    sites, several GB as float64), so instead of loading it whole this opens it
    lazily, keeps only ``site_ids`` (when given), and reads one variable at a time
    as float32 before aggregating with ``aggregate_operational_gefs``. The result
    matches aggregating the fully loaded file, apart from float32 rounding.
    """
    ds = xr.open_dataset(path, engine="netcdf4", decode_timedelta=True)
    if site_ids:
        keep = [s for s in ds.site_id.values if str(s) in {str(x) for x in site_ids}]
        if not keep:
            raise ValueError(f"None of site_ids {site_ids} are in {path} (has {list(ds.site_id.values)})")
        ds = ds.sel(site_id=keep)
    loaded = xr.Dataset({var: ds[var].load().astype("float32") for var in variables})
    ds.close()
    return aggregate_operational_gefs(ds=loaded, out_vars=variables)


def aggregate_analysis_gefs(
        ds,
        out_vars
):
    """
    aggregating analysis GEFS to daily predictions
    """
    mean_vars = ["downward_long_wave_radiation_flux_surface", "downward_short_wave_radiation_flux_surface", "temperature_2m", "precipitation_surface", "total_cloud_cover_atmosphere", "wind_u_10m", "wind_v_10m"]
    max_vars = ["maximum_temperature_2m"]
    min_vars = ["minimum_temperature_2m"]
    ds_mean = ds[mean_vars].resample(time='1d').mean()
    ds_max = ds[max_vars].resample(time='1d').max()
    ds_min = ds[min_vars].resample(time='1d').min()

    out_ds = (
        xr.merge([ds_mean, ds_max, ds_min])[out_vars]
    )

    return out_ds


def calculate_forecast_uncertainty(ds, met_vars, include_median=True):
    """
    Calculate PI90 (90% prediction interval) from GEFS ensemble.

    Parameters
    ----------
    ds : xarray.Dataset
        Forecast data with ensemble_member dimension
    met_vars : list
        Meteorological variable names
    include_median : bool
        Whether to also calculate and include median values (default True)

    Returns
    -------
    ds_with_uncertainty : xarray.Dataset
        Dataset with PI90 variables (and optionally median) for each met var
    """
    # Calculate quantiles across ensemble
    ds_q05 = ds[met_vars].quantile(0.05, dim='ensemble_member')
    ds_q95 = ds[met_vars].quantile(0.95, dim='ensemble_member')

    # PI90 = Q95 - Q05
    ds_pi90 = ds_q95 - ds_q05

    # Create new dataset with PI90 variables
    pi90_vars = {}
    for var in met_vars:
        pi90_vars[f"{var}_pi90"] = ds_pi90[var]

    ds_with_uncertainty = xr.Dataset(pi90_vars)

    # Optionally compute median for the central estimate
    if include_median:
        ds_median = ds[met_vars].quantile(0.5, dim='ensemble_member')
        for var in met_vars:
            ds_with_uncertainty[var] = ds_median[var]

    return ds_with_uncertainty


def calculate_climatology_with_uncertainty(ds_historical, met_vars, pi90_multiplier=4.0):
    """
    Calculate day-of-year climatology with large uncertainty bounds.

    This provides a fallback for historical periods without GEFS operational
    forecasts, using climatological means with large uncertainty to signal
    low forecast reliability to the model.

    Parameters
    ----------
    ds_historical : xarray.Dataset
        Historical meteorological observations (from GEFS analysis)
    met_vars : list
        Meteorological variable names
    pi90_multiplier : float
        Multiplier for std to get PI90 (default 4.0 = ~2 std on each side)

    Returns
    -------
    climatology : xarray.Dataset
        Day-of-year means and PI90 for each variable
    """
    # Group by day of year
    ds_doy = ds_historical.groupby('time.dayofyear')

    # Calculate mean and std for each DOY
    clim_mean = ds_doy.mean(dim='time')
    clim_std = ds_doy.std(dim='time')

    # PI90 = multiplier * std (large uncertainty for climatology)
    clim_pi90 = clim_std * pi90_multiplier

    climatology = xr.Dataset()
    for var in met_vars:
        climatology[var] = clim_mean[var]
        climatology[f"{var}_pi90"] = clim_pi90[var]

    return climatology

def data_prep_encoder_decoder(config_file, root_dir=None):
    """
    Prepares the dataset for encoder-decoder LSTM training.

    This function creates separate encoder and decoder input arrays:
    - Encoder: past observations (365 days by default)
    - Decoder: future forecasts (10 days by default) with climatology fallback

    Parameters
    ----------
    config_file : str
        Path to model configuration YAML file
    root_dir : str, optional
        Root directory for data paths

    Returns
    -------
    dict : Dictionary containing:
        - 'x_encoder_train': Training encoder inputs
        - 'x_decoder_train': Training decoder inputs
        - 'y_train': Training targets (per forecast day)
        - 'x_encoder_val': Validation encoder inputs
        - 'x_decoder_val': Validation decoder inputs
        - 'y_val': Validation targets
        - Scaling parameters and metadata
    """
    config = load_config(config_file)

    site_metadata_xr = get_metadata(
        config['site_metadata_url'],
        config['savoy_metadata_file'],
        config['stackpoole_metadata_file'],
        sites=config['sites_to_include']
    )

    start_time = np.datetime64(config['min_date'])
    end_time = np.datetime64(config['max_date'])

    # Load data sources
    print("Loading observation data...")
    obs_xr = xr.load_dataset(
        filename_or_obj=config['obs_local_file'],
        engine="netcdf4",
        chunks=None
    )

    print("Loading GEFS analysis (historical met)...")
    gefs_xr = xr.load_dataset(
        filename_or_obj=config['gefs_local_file'],
        engine="netcdf4",
        chunks=None
    )
    gefs_xr = aggregate_analysis_gefs(ds=gefs_xr, out_vars=config['x_vars'])

    print("Loading GEFS operational forecasts...")
    gefs_operational_xr = (
        load_operational_gefs_daily(
            path=config['gefs_operational_local_file'],
            variables=config['x_vars'],
            site_ids=config.get('site_ids_to_include'),
        )
        .rename({"init_time": "time"})
    )

    # Calculate PI90 from operational GEFS ensemble for decoder inputs
    print("Calculating PI90 from GEFS operational ensemble...")
    gefs_pi90_xr = calculate_forecast_uncertainty(
        ds=gefs_operational_xr,
        met_vars=config['x_vars'],
        include_median=False
    )
    # Merge PI90 into operational dataset
    gefs_operational_with_pi90 = xr.merge([gefs_operational_xr, gefs_pi90_xr])

    print("Loading hydrology data...")
    hydro_xr = xr.load_dataset(
        filename_or_obj=config['hydro_local_file'],
        engine="netcdf4",
        chunks=None
    )

    print("Loading static features...")
    static_xr = pull_static_features(
        site_metadata=site_metadata_xr,
        variables=config['x_vars_static']
    )

    # Combine all data
    data_xr = (
        xr.combine_by_coords(
            [gefs_xr, obs_xr, hydro_xr, static_xr],
            join='left'
        )
        .where(lambda x: x.source_dataset.isin(config['sites_to_include']), drop=True)
    )
    if config.get('site_ids_to_include'):
        data_xr = data_xr.where(data_xr.site_id.isin(config['site_ids_to_include']), drop=True)

    # Keep met/climatology and operational forecast datasets on the same site set as training data.
    # This prevents downstream PI90 blending shape mismatches when metadata-based filtering drops sites.
    common_sites = data_xr.site_id.values
    gefs_xr = gefs_xr.sel(site_id=common_sites)
    gefs_operational_with_pi90 = gefs_operational_with_pi90.sel(site_id=common_sites)

    # Broadcast static features to all timesteps
    data_xr[config['x_vars_static']] = data_xr[config['x_vars_static']].broadcast_like(data_xr.time)

    # Calculate climatology with uncertainty for decoder fallback
    print("Calculating climatology with uncertainty for decoder...")
    base_met_vars = config['x_vars'].copy()
    climatology_xr = calculate_climatology_with_uncertainty(
        ds_historical=gefs_xr,
        met_vars=base_met_vars,
        pi90_multiplier=config.get('climatology_pi90_multiplier', 4.0)
    )

    # Add PI90 to data_xr using climatology (for encoder-decoder training)
    for var in base_met_vars:
        pi90_var = f"{var}_pi90"
        clim_pi90_aligned = climatology_xr[pi90_var].sel(
            dayofyear=data_xr['time'].dt.dayofyear
        )
        data_xr[pi90_var] = clim_pi90_aligned.broadcast_like(data_xr.time)

    # Handle lagged target variable if needed
    encoder_seq_len = config.get('encoder_seq_len', 365)
    lag_days = config.get('lag_days', 1)
    lag_source = chla_lag_source(config)
    if lag_source:
        print(f"Adding lagged chlorophyll features ({lag_source} source)...")
    if lag_source == 'realtime':
        # Real-time observed chla (VERA4cast Chla_ugL_mean). Reindex onto the
        # existing time axis so the encoder lookback isn't extended with
        # all-NaN leading rows.
        lagged_xr = build_chla_lag(
            url=chla_lag_url(config),
            start_time=start_time,
            end_time=end_time,
            lag_days=chla_lag_days(config),
            site_ids=config.get('site_ids_to_include'),
        ).reindex(time=data_xr.time)
        data_xr = xr.merge([data_xr, lagged_xr], join='left')
    elif lag_source == 'noAR':
        # Need predicted chla going back far enough for encoder history + lag
        pred_start_time = start_time - np.timedelta64(encoder_seq_len + lag_days + 10, 'D')
        pred_chl_xr = pull_predicted_chl(
            data_file=config['lstm_noAR_local_file'],
            start_time=pred_start_time,
            end_time=end_time
        )
        obs_chl_xr = (
            data_xr['chla']
            .to_dataset()
            # Observation uncertainty: constant 5% CV
            # SD = 5% of measurement, PI90 = 3.29 × SD
            .assign(chla_uncertainty=lambda x: 3.29 * 0.05 * np.abs(x.chla))
        )
        # Reindex pred_chl to match obs_chl coordinates for proper combine_first
        # This ensures site_id order and time values align exactly
        pred_chl_aligned = pred_chl_xr.reindex_like(obs_chl_xr, method=None)
        obs_pred_chl_xr = obs_chl_xr.combine_first(pred_chl_aligned)

        lagged_xr = (
            obs_pred_chl_xr[['chla', 'chla_uncertainty']]
            .shift(time=config['lag_days'])
            .rename({
                "chla": "chla_lagged",
                "chla_uncertainty": "chla_uncertainty_lagged"
            })
        )
        data_xr = (
            xr.combine_by_coords([data_xr, lagged_xr], join='left')
            .isel(time=slice(config['lag_days'], None))
        )

    # Define encoder and decoder variables
    encoder_vars = config['x_vars'].copy()
    if config.get('hydro_vars'):
        encoder_vars = encoder_vars + config['hydro_vars']
    encoder_vars = encoder_vars + config['x_vars_static']
    if lag_source:
        encoder_vars = encoder_vars + ['chla_lagged', 'chla_uncertainty_lagged']

    # Decoder: met forecasts + PI90 + static features + lagged chla (for autoregressive)
    decoder_vars = config['x_vars'].copy()
    # Add PI90 variables
    pi90_vars = [f"{var}_pi90" for var in base_met_vars]
    decoder_vars = decoder_vars + pi90_vars
    if config.get('decoder_include_static', True):
        decoder_vars = decoder_vars + config['x_vars_static']

    # Add lagged chla to decoder only if autoregressive mode is enabled
    # When decoder_autoregressive=False, decoder doesn't get lagged chla to avoid
    # discontinuity between encoder and decoder (implements TODO Option 1)
    if lag_source and config.get('decoder_autoregressive', False):
        decoder_vars = decoder_vars + ['chla_lagged', 'chla_uncertainty_lagged']

    # Last observed chla (available at forecast time) on every decoder day, so the
    # decoder starts from the current state rather than having to carry it over
    # from the encoder.
    if config.get('decoder_last_chla', False):
        if not lag_source:
            raise ValueError("decoder_last_chla needs chla history in the encoder (chla_lag or lag_target).")
        decoder_vars = decoder_vars + [LAST_CHLA_VAR]

    target_vars = config['y_vars']

    encoder_seq_len = config.get('encoder_seq_len', 365)
    decoder_seq_len = config.get('decoder_seq_len', 10)
    # Same driver age as operation (build_forecast_data.py): a sample initialized on
    # R is driven by the GEFS forecast issued on R - met_lag.
    met_lag = met_driver_lag_days(config)

    print(f"Creating encoder-decoder samples...")
    print(f"  Encoder: {encoder_seq_len} days, {len(encoder_vars)} features")
    print(f"  Decoder: {decoder_seq_len} days, {len(decoder_vars)} features")
    print(f"  Encoder vars: {encoder_vars}")
    print(f"  Decoder vars: {decoder_vars}")

    # Get log-transform setting
    log_transform_target = config.get('log_transform_target', False)
    if log_transform_target:
        print("Log-transforming target variable (chlorophyll)")

    # Create training samples
    # Pass operational GEFS with PI90 for blending with climatology
    print("Creating training samples...")
    train_data = create_encoder_decoder_samples_for_periods(
        start_dates=config['start_date_train'],
        end_dates=config['end_date_train'],
        data_xr=data_xr,
        encoder_vars=encoder_vars,
        decoder_vars=decoder_vars,
        target_vars=target_vars,
        climatology_xr=climatology_xr,
        gefs_operational_xr=gefs_operational_with_pi90,  # Use blended PI90
        encoder_seq_len=encoder_seq_len,
        decoder_seq_len=decoder_seq_len,
        spatial_idx_name=config['spatial_idx_name'],
        time_idx_name=config['time_idx_name'],
        offset=1,
        log_transform_target=log_transform_target,
        met_lag_days=met_lag
    )

    # Create validation samples
    print("Creating validation samples...")
    val_data = create_encoder_decoder_samples_for_periods(
        start_dates=config['start_date_val'],
        end_dates=config['end_date_val'],
        data_xr=data_xr,
        encoder_vars=encoder_vars,
        decoder_vars=decoder_vars,
        target_vars=target_vars,
        climatology_xr=climatology_xr,
        gefs_operational_xr=gefs_operational_with_pi90,  # Use blended PI90
        encoder_seq_len=encoder_seq_len,
        decoder_seq_len=decoder_seq_len,
        spatial_idx_name=config['spatial_idx_name'],
        time_idx_name=config['time_idx_name'],
        offset=1,
        log_transform_target=log_transform_target,
        met_lag_days=met_lag
    )

    # Create test samples (if test dates are configured)
    # Don't filter NaN targets for test data - we want predictions for all sites
    test_data = None
    if config.get('start_date_test') and config.get('end_date_test'):
        print("Creating test samples...")
        test_data = create_encoder_decoder_samples_for_periods(
            start_dates=config['start_date_test'],
            end_dates=config['end_date_test'],
            data_xr=data_xr,
            encoder_vars=encoder_vars,
            decoder_vars=decoder_vars,
            target_vars=target_vars,
            climatology_xr=climatology_xr,
            gefs_operational_xr=gefs_operational_with_pi90,  # Use blended PI90
            encoder_seq_len=encoder_seq_len,
            decoder_seq_len=decoder_seq_len,
            spatial_idx_name=config['spatial_idx_name'],
            time_idx_name=config['time_idx_name'],
            offset=1,
            filter_nan_targets=False,  # Keep all samples for test/prediction
            log_transform_target=log_transform_target,
            met_lag_days=met_lag
        )

    # Scale the data
    print("Scaling data...")
    train_data_scaled = scale_encoder_decoder(train_data)

    val_data_scaled = scale_encoder_decoder(
        val_data,
        encoder_std=train_data_scaled['encoder_std'],
        encoder_mean=train_data_scaled['encoder_mean'],
        decoder_std=train_data_scaled['decoder_std'],
        decoder_mean=train_data_scaled['decoder_mean'],
        target_std=train_data_scaled['target_std'],
        target_mean=train_data_scaled['target_mean']
    )

    # Scale test data if available
    test_data_scaled = None
    if test_data is not None:
        test_data_scaled = scale_encoder_decoder(
            test_data,
            encoder_std=train_data_scaled['encoder_std'],
            encoder_mean=train_data_scaled['encoder_mean'],
            decoder_std=train_data_scaled['decoder_std'],
            decoder_mean=train_data_scaled['decoder_mean'],
            target_std=train_data_scaled['target_std'],
            target_mean=train_data_scaled['target_mean']
        )

    # Prepare output dictionary
    all_data = {
        # Training data
        'x_encoder_train': train_data_scaled['x_encoder'],
        'x_decoder_train': train_data_scaled['x_decoder'],
        'y_train': train_data_scaled['y'],
        'init_dates_train': train_data_scaled['init_dates'],
        'site_ids_train': train_data_scaled['site_ids'],

        # Validation data
        'x_encoder_val': val_data_scaled['x_encoder'],
        'x_decoder_val': val_data_scaled['x_decoder'],
        'y_val': val_data_scaled['y'],
        'init_dates_val': val_data_scaled['init_dates'],
        'site_ids_val': val_data_scaled['site_ids'],

        # Scaling parameters
        'encoder_mean': train_data_scaled['encoder_mean'],
        'encoder_std': train_data_scaled['encoder_std'],
        'decoder_mean': train_data_scaled['decoder_mean'],
        'decoder_std': train_data_scaled['decoder_std'],
        'target_mean': train_data_scaled['target_mean'],
        'target_std': train_data_scaled['target_std'],

        # Variable names
        'encoder_vars': train_data_scaled['encoder_vars'],
        'decoder_vars': train_data_scaled['decoder_vars'],
        'target_vars': train_data_scaled['target_vars'],

        # Configuration
        'encoder_seq_len': encoder_seq_len,
        'decoder_seq_len': decoder_seq_len,

        # Log-transform flag (for back-transformation in predictions)
        'target_log_transformed': train_data_scaled.get('target_log_transformed', False),

        # For compatibility with existing code
        'x_vars': np.array(encoder_vars),  # Encoder vars as default x_vars
    }

    # Add observation uncertainty (PI90) if available
    if 'y_obs_pi90' in train_data_scaled:
        all_data['y_obs_pi90_train'] = train_data_scaled['y_obs_pi90']
        print(f"  Included observation uncertainty (PI90) for training data")
    if 'y_obs_pi90' in val_data_scaled:
        all_data['y_obs_pi90_val'] = val_data_scaled['y_obs_pi90']
        print(f"  Included observation uncertainty (PI90) for validation data")

    # Add test data if available
    if test_data_scaled is not None:
        all_data['x_encoder_test'] = test_data_scaled['x_encoder']
        all_data['x_decoder_test'] = test_data_scaled['x_decoder']
        all_data['y_test'] = test_data_scaled['y']
        all_data['init_dates_test'] = test_data_scaled['init_dates']
        all_data['site_ids_test'] = test_data_scaled['site_ids']
        if 'y_obs_pi90' in test_data_scaled:
            all_data['y_obs_pi90_test'] = test_data_scaled['y_obs_pi90']
            print(f"  Included observation uncertainty (PI90) for test data")

    # Save to file
    model_id = get_model_id(config)
    output_file = os.path.join(training_data_dir(config), f'{model_id}.npz')
    check_no_overwrite([output_file], config)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    print(f"Saving encoder-decoder training data to {output_file}")
    np.savez_compressed(output_file, **all_data)

    test_count = test_data['x_encoder'].shape[0] if test_data is not None else 0
    print(f"Done! Created {train_data['x_encoder'].shape[0]} training samples, "
          f"{val_data['x_encoder'].shape[0]} validation samples, "
          f"{test_count} test samples")

    return all_data


if __name__ == '__main__':
    # configuration file path
    config_file = "../model_config.yml"

    # Check model type and run appropriate data prep
    config = load_config(config_file)
    if config.get('model_type') == 'encoder_decoder':
        data_prep_encoder_decoder(config_file)
    else:
        data_prep(config_file)



