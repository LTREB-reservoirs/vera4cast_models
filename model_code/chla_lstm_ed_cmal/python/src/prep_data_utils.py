import pandas as pd
import xarray as xr
import numpy as np
import sys

def scale(dataset, std=None, mean=None):
    """
    scale the data so it has a standard deviation of 1 and a mean of zero
    :param dataset: [xr dataset] input or output data
    :param std: [xr dataset] standard deviation if scaling test data with dims
    :param mean: [xr dataset] mean if scaling test data with dims
    :return: scaled data with original dims
    """

    if not isinstance(std, xr.Dataset) or not isinstance(mean, xr.Dataset):
        std = dataset.std(skipna=True)
        mean = dataset.mean(skipna=True)

    # adding small number in case there is a std of zero
    scaled = (dataset - mean) / (std + 1e-10)

    check_if_finite(std)
    check_if_finite(mean)
    return scaled, std, mean



# from river-dl
def split_into_batches(data_array, seq_len=365, offset=1.0,
                       fill_batch=True, fill_nan=False, fill_time=False,
                       fill_pad=False):
    """
    split training data into batches with size of seq_len
    :param data_array: [numpy array] array of training data with dims [nseg,
    ndates, nfeat]
    :param seq_len: [int] length of sequences (e.g., 365)
    :param offset: [float] How to offset the batches. Values < 1 are taken as fractions, (e.g., 0.5 means that
    the first batch will be 0-365 and the second will be 182-547), values > 1 are used as a constant number of
    observations to offset by.
    :param fill_batch: [bool] when True, batches are filled to match the seq_len.
    This ensures that data are not dropped when the data_array length is not
    a multiple of the seq_len. Data are added to the end of the sequence.
    When False, data may be dropped.
    :param fill_nan: [bool] When True, filled in data are np.nan (e.g., because
    data_array is observation data that should not contribute to the loss).
    When False, filled in data are replicates of the previous timesteps.
    :param fill_time: [bool] When True, filled in data are time indices that
    follow in sequence from the previous timesteps. When False, filled in data
    are replicates of the previous timesteps.
    :param fill_pad: [bool] When True, the returned data are bool indicating
    True when it is padded and False otherwise. When False, filled in data
    are based on other fill_ rules.
    :return: [numpy array] batched data with dims [nbatches, nseg, seq_len
    (batch_size), nfeat]
    """
    if offset>1:
        period = int(offset)
    else:
        period = int(offset*seq_len)

    nsteps = data_array.shape[1]
    num_batches = nsteps//period
    if fill_batch:
        final_batch_shape_check = nsteps - period*num_batches
        if (final_batch_shape_check != seq_len):
            #append timesteps to the data_array
            #Determine how many timesteps to replicate to get a full batch
            num_rep_steps = seq_len - final_batch_shape_check

            if fill_nan:
                #fill in with nan values
                nan_array = np.empty((data_array.shape[0],
                                      num_rep_steps,
                                      data_array.shape[2]))
                nan_array.fill(np.nan)
                data_array = np.concatenate((data_array,
                                             nan_array),
                                            axis = 1)
            elif fill_pad:
                #fill in with True for padded values and False otherwise
                False_array = np.empty((data_array.shape[0],
                                      data_array.shape[1],
                                      data_array.shape[2]),
                                      dtype=bool)
                True_array = np.empty((data_array.shape[0],
                                      num_rep_steps,
                                      data_array.shape[2]),
                                      dtype=bool)
                False_array.fill(False)
                True_array.fill(True)
                data_array = np.concatenate((False_array,
                                             True_array),
                                            axis = 1)
            else:
                #fill in by replicating the previous timesteps in the data_array
                if fill_time:
                    #data are an np.datetime64 object. These must be unique, so
                    #cannot be replicated. Add timesteps sequentially
                    fill_dates_array = data_array[:,(nsteps-num_rep_steps):nsteps,:].copy()
                    #add num_rep_steps to each index.
                    # Sending the smallest possible sample of data_array to the function
                    time_unit = get_time_unit(data_array[0:2,0:2,0:2])
                    fill_dates_array = fill_dates_array[:,:,:] + np.timedelta64(num_rep_steps, time_unit)

                    data_array = np.concatenate((data_array,
                                                 fill_dates_array),
                                                axis = 1)
                else:
                    data_array = np.concatenate((data_array,
                                                 data_array[:,(nsteps-num_rep_steps):nsteps,:]),
                                                axis = 1)

            num_batches = num_batches+1

        elif fill_pad:
            #return array of False - no padded data
            False_array = np.empty((data_array.shape[0],
                                  data_array.shape[1],
                                  data_array.shape[2]),
                                  dtype=bool)
            False_array.fill(False)
            data_array = False_array

    elif fill_pad:
        #return array of False - no padded data
        False_array = np.empty((data_array.shape[0],
                              data_array.shape[1],
                              data_array.shape[2]),
                              dtype=bool)
        False_array.fill(False)
        data_array = False_array


    combined=[]
    for i in range(num_batches+1):
        idx = int(period*i)
        batch = data_array[:,idx:idx+seq_len,...]
        combined.append(batch)
    combined = [b for b in combined if b.shape[1]==seq_len]
    combined = np.asarray(combined)
    return combined


def convert_batch_reshape(
    dataset,
    spatial_idx_name="site_id",
    time_idx_name="date",
    seq_len=365,
    offset=1.0,
    fill_batch=True,
    fill_nan=False,
    fill_time=False,
    fill_pad=False
):
    """
    convert xarray dataset into numpy array, swap the axes, batch the array and
    reshape for training
    :param dataset: [xr dataset] data to be batched
    :param spatial_idx_name: [str] name of column that is used for spatial
        index (e.g., 'site_id')
    :param time_idx_name: [str] name of column that is used for temporal index
        (usually 'time')
    :param seq_len: [int] length of sequences (e.g., 365)
    :param offset: [float] 0-1, how to offset the batches (e.g., 0.5 means that
    the first batch will be 0-365 and the second will be 182-547)
    :param fill_batch: [bool] when True, batches are filled to match the seq_len.
    This ensures that data are not dropped when the data_array length is not
    a multiple of the seq_len. Data are added to the end of the sequence.
    When False, data may be dropped.
    :param fill_nan: [bool] When True, filled in data are np.nan (e.g., because
    data_array is observation data that should not contribute to the loss).
    :param fill_time: [bool] When True, filled in data are time indices that
    follow in sequence from the previous timesteps. When False, filled in data
    are replicates of the previous timesteps.
    :param fill_pad: [bool] When True, the returned data are bool indicating
    True when it is padded and False otherwise. When False, filled in data
    are based on other fill_ rules.
    :return: [numpy array] batched and reshaped dataset
    """
    # If there is no dataset (like if a test or validation set is not supplied)
    # just return None
    if not dataset:
        return None

    if fill_batch:
        continuous_start_inds = []
        #Check if there are gaps in the timeseries as a result of using a
        # discontinuous partition.
        #identify all gaps greater than the 1st timestep
        time_diff = np.diff(dataset[time_idx_name])
        timestep_1 = time_diff[0]
        if any(time_diff != timestep_1):
            #fill those gaps based on the sequence length.
            gap_timesteps = np.delete(np.unique(time_diff),
                                      np.where(np.unique(time_diff) == timestep_1))
            for t in gap_timesteps:
                #using [0] to return an array
                gap_ind = np.where(time_diff == t)[0]
                #there can be multiple gaps of the same length
                for i in gap_ind:
                    #determine if the gap is longer than the sequence length
                    date_before_gap = dataset[time_idx_name][i].values
                    next_date = dataset[time_idx_name][i+1].values
                    #gap length in the same unit as the timestep
                    gap_length = int((next_date - date_before_gap)/timestep_1)
                    if gap_length < seq_len:
                        #I originally had this as a sys.exit, but did not want
                        # to force this condition.
                        print("The gap between this partition's continuous time periods is less than the sequence length")

                    #get the start date indices. These are used to split the
                    # dataset before creating batches
                    continuous_start_inds.append(i+1)

            continuous_start_inds = np.sort(continuous_start_inds)

    # convert xr.dataset to numpy array
    dataset = dataset.transpose(spatial_idx_name, time_idx_name)

    arr = dataset.to_array().values

    # if the dataset is empty, just return it as is
    if dataset[time_idx_name].size == 0:
        return arr

    # before [nfeat, nseg, ndates]; after [nseg, ndates, nfeat]
    # this is the order that the split into batches expects
    arr = np.moveaxis(arr, 0, -1)

    # batch the data
    # after [nbatch, nseg, seq_len, nfeat]
    if fill_batch:
        if len(continuous_start_inds) != 0:
            #using a discontinuous partition. create a set of batches for each
            # continuous period in this partion and join
            for g in range(len(continuous_start_inds)+1):
                if g == 0:
                    arr_g = arr[:,0:continuous_start_inds[g],:].copy()

                    batched = split_into_batches(arr_g, seq_len=seq_len, offset=offset,
                                                 fill_batch=fill_batch, fill_nan=fill_nan,
                                                 fill_time=fill_time, fill_pad=fill_pad)

                else:
                    if g == len(continuous_start_inds):
                        arr_g = arr[:,continuous_start_inds[g-1]:arr.shape[1],:].copy()
                    else:
                        arr_g = arr[:,continuous_start_inds[g-1]:continuous_start_inds[g],:].copy()

                    batched_g = split_into_batches(arr_g, seq_len=seq_len, offset=offset,
                                                   fill_batch=fill_batch, fill_nan=fill_nan,
                                                   fill_time=fill_time, fill_pad=fill_pad)
                    batched = np.append(batched, batched_g, axis = 0)
        else:
            batched = split_into_batches(arr, seq_len=seq_len, offset=offset,
                                         fill_batch=fill_batch, fill_nan=fill_nan,
                                         fill_time=fill_time, fill_pad=fill_pad)
    else:
        batched = split_into_batches(arr, seq_len=seq_len, offset=offset,
                                     fill_batch=fill_batch, fill_nan=fill_nan,
                                     fill_time=fill_time, fill_pad=fill_pad)

    # reshape data
    # after [nbatch * nseg, seq_len, nfeat]
    reshaped = reshape_for_training(batched)
    return reshaped

def separate_trn_tst(
    dataset,
    time_idx_name,
    train_start_date,
    train_end_date,
    val_start_date=None,
    val_end_date=None,
    test_start_date=None,
    test_end_date=None,
    spatial_idx_name="site_id",
    y_vars=None,
    withheld_ids=None,
):
    """
    separate the train data from the test data according to the start and end
    dates. This assumes your training data is in one continuous block. Be aware,
    if your train/test/val partitions are discontinuous (composed of multiple
    periods), depending on your sequence length and how the data line up, you
    could end up with sequences starting in one period and ending in another.
    The breaking up of sequences would happen in the `convert_batch_reshape`
    function
    :param dataset: [xr dataset] input or output data with dims
    :param time_idx_name: [str] name of column that is used for temporal index
        (usually 'time')
    :param train_start_date: [str or list] fmt: "YYYY-MM-DD"; date(s) to start
    train period (can have multiple discontinuous periods)
    :param train_end_date: [str or list] fmt: "YYYY-MM-DD"; date(s) to end train
     period (can have multiple discontinuous periods)
    :param val_start_date: [str or list] fmt: "YYYY-MM-DD"; date(s) to start
     validation period (can have multiple discontinuous periods)
    :param val_end_date: [str or list] fmt: "YYYY-MM-DD"; date(s) to end
    validation period (can have multiple discontinuous periods)
    :param test_start_date: [str or list] fmt: "YYYY-MM-DD"; date(s) to start
    test period (can have multiple discontinuous periods)
    :param test_end_date: [str or list] fmt: "YYYY-MM-DD"; date(s) to end test
    period (can have multiple discontinuous periods)
    :param withheld_ids: [str or list] id(s) to withhold from training and validation
     setting the observations to NA's
    :return: [tuple] separated data
    """
    train = sel_partition_data(
        dataset, time_idx_name, train_start_date, train_end_date, spatial_idx_name, y_vars, withheld_ids
    )

    if val_start_date and val_end_date:
        val = sel_partition_data(
            dataset, time_idx_name, val_start_date, val_end_date, spatial_idx_name, y_vars, withheld_ids
        )

    elif val_start_date and not val_end_date:
        raise ValueError("With a val_start_date a val_end_date must be given")
    elif val_end_date and not val_start_date:
        raise ValueError("With a val_end_date a val_start_date must be given")
    else:
        val = None

    if test_start_date and test_end_date:
        test = sel_partition_data(
            dataset, time_idx_name, test_start_date, test_end_date
        )
    elif test_start_date and not test_end_date:
        raise ValueError("With a test_start_date a test_end_date must be given")
    elif test_end_date and not test_start_date:
        raise ValueError("With a test_end_date a test_start_date must be given")
    else:
        test = None

    return train, val, test


def sel_partition_data(dataset,
                       time_idx_name,
                       start_dates,
                       end_dates,
                       spatial_idx_name="site_id",
                       y_vars=None,
                       withheld_ids=None
):
    """
    select the data from a date range or a set of date ranges
    :param dataset: [xr dataset] input or output data with date dimension
    :param time_idx_name: [str] name of column that is used for temporal index
        (usually 'time')
    :param start_dates: [str or list] fmt: "YYYY-MM-DD"; date(s) to start period
    (can have multiple discontinuos periods)
    :param end_dates: [str or list] fmt: "YYYY-MM-DD"; date(s) to end period
    (can have multiple discontinuos periods)
    :return: dataset of just those dates
    """
   # if it just one date range
    if isinstance(start_dates, str):
        if isinstance(end_dates, str):
            out = dataset.sel({time_idx_name: slice(start_dates, end_dates)})
        else:
            raise ValueError("start_dates is str but not end_date")
    # if it's a list of date ranges
    elif isinstance(start_dates, list) or isinstance(start_dates, tuple):
        if len(start_dates) == len(end_dates):
            data_list = []
            for i in range(len(start_dates)):
                date_slice = slice(start_dates[i], end_dates[i])
                data_list.append(dataset.sel({time_idx_name: date_slice}))
            out = xr.concat(data_list, dim=time_idx_name)
        else:
            raise ValueError("start_dates and end_dates must have same length")
    else:
        raise ValueError("start_dates must be either str, list, or tuple")
    # if there are withheld ids, then set those observations to NA's
    if withheld_ids:
        out = withhold_sites(out, spatial_idx_name, y_vars, withheld_ids)

    return out


def withhold_sites(dataset,
                  spatial_idx_name,
                  y_vars,
                  withheld_ids
):
    """
    Sets y_vars to NA's for sites we want to withhold during training.
    :param dataset: [xr dataset] input dataset
    :param spatial_idx_name:
    :param y_vars:
    :param withheld_ids: [str or list] id(s) to withhold from training and validation
     setting the observations to NA's
    """
    spatial_mask = np.logical_not(np.isin(dataset[spatial_idx_name], withheld_ids))
    len_dates = dataset.sizes['Date']
    spatial_mask_rep = np.tile(spatial_mask, (len_dates, 1))
    spatial_mask_rep = spatial_mask_rep.swapaxes(0,1)

    masked_y_vars = dataset[y_vars].where(spatial_mask_rep, np.nan)

    dataset.update(masked_y_vars) # update y_vars in the original xarray with the masked y_vars for withheld sites
    return dataset


def reshape_for_training(data):
    """
    reshape the data for training
    :param data: training data (either x or y_dataset or mask) dims: [nbatch, nseg,
    len_seq, nfeat/nout]
    :return: reshaped data [nbatch * nseg, len_seq, nfeat/nout]
    """
    n_batch, n_seg, seq_len, n_feat = data.shape
    return np.reshape(data, [n_batch * n_seg, seq_len, n_feat])

def check_if_finite(xarr):
    assert np.isfinite(xarr.to_array().values).all()


def coord_as_reshaped_array(
    dataset,
    coord_name,
    spatial_idx_name="StaID",
    time_idx_name="Date",
    seq_len=365,
    offset=1.0,
    fill_batch=True,
    fill_nan=False,
    fill_time=False,
    fill_pad=False
):
    """
    convert an xarray coordinate to an xarray data array and reshape that array
    :param dataset:
    :param coord_name: [str] the name of the coordinate to convert/reshape
    :param spatial_idx_name: [str] name of column that is used for spatial
        index (e.g., 'StaID')
    :param time_idx_name: [str] name of column that is used for temporal index
        (usually 'Date')
    :param seq_len: [int] length of sequences (e.g., 365)
    :param offset: [float] 0-1, how to offset the batches (e.g., 0.5 means that
    the first batch will be 0-365 and the second will be 182-547)
    :param fill_batch: [bool] when True, batches are filled to match the seq_len.
    This ensures that data are not dropped when the data_array length is not
    a multiple of the seq_len. Data are added to the end of the sequence.
    When False, data may be dropped.
    :param fill_nan: [bool] When True, filled in data are np.nan (e.g., because
    data_array is observation data that should not contribute to the loss).
    When False, filled in data are replicates of the previous timesteps.
    :param fill_time: [bool] When True, filled in data are time indices that
    follow in sequence from the previous timesteps. When False, filled in data
    are replicates of the previous timesteps.
    :param fill_pad: [bool] When True, the returned data are bool indicating
    True when it is padded and False otherwise. When False, filled in data
    are based on other fill_ rules.
    :return:
    """
    # If there is no dataset (like if a test or validation set is not supplied)
    # just return None
    if not dataset:
        return None

    # I need one variable name. It can be any in the dataset, but I'll use the
    # first
    first_var = next(iter(dataset.data_vars.keys()))
    coord_array = xr.broadcast(dataset[coord_name], dataset[first_var])[0]
    new_var_name = coord_name + "1"
    dataset[new_var_name] = coord_array
    reshaped_np_arr = convert_batch_reshape(
        dataset[[new_var_name]],
        spatial_idx_name,
        time_idx_name,
        seq_len=seq_len,
        offset=offset,
        fill_batch=fill_batch,
        fill_nan=fill_nan,
        fill_time=fill_time,
        fill_pad=fill_pad
    )
    return reshaped_np_arr

def get_time_unit(data_array):
    '''
    Function to get the timestep unit from a numpy.datetime64 array column

    :param data_array: [np 3D array] the array's second axis must be time
    in np.datetime64 format with nanoseconds specified (default).

    returns the unit of the timestep (day, month, etc.)
    '''
    time_delta = data_array[0,1,0] - data_array[0,0,0]
    time_unit = np.datetime_data(time_delta)[0]
    if time_unit != 'ns':
        sys.exit('time unit must be provided as YYYY-MM-DDT:HH:MM:SS.000000000 nanoseconds')

    if time_delta == 86400000000000:
        time_unit = 'D'
    elif time_delta == 24000000000:
        time_unit = 'h'
    else:
        sys.exit('time_delta does not correspond to 1 h or D')

    return(time_unit)


# =============================================================================
# Encoder-Decoder Data Preparation Functions
# =============================================================================

def create_encoder_decoder_samples(
    data_xr,
    encoder_vars,
    decoder_vars,
    target_vars,
    climatology_xr=None,
    gefs_operational_xr=None,
    encoder_seq_len=365,
    decoder_seq_len=10,
    spatial_idx_name="site_id",
    time_idx_name="time",
    offset=1,
    start_date=None,
    end_date=None,
    include_obs_uncertainty=True,
    filter_nan_targets=True,
    log_transform_target=False
):
    """
    Create aligned encoder-decoder sample pairs for training.

    For each valid forecast initialization date, creates:
    - Encoder input: past `encoder_seq_len` days of observations
    - Decoder input: future `decoder_seq_len` days of forecasts
    - Target: actual observations for decoder period
    - Target uncertainty: PI90 of observation uncertainty for loss weighting

    Decoder inputs use blended data:
    - For dates with operational GEFS: uses ensemble median and actual PI90
    - For historical dates: uses climatology means and large PI90 (4×std)

    Observation uncertainty (PI90) is calculated as:
    - For observed chlorophyll: PI90 = 3.29 × 0.05 × measurement (constant 5% CV)
    - For predicted chlorophyll (from noAR model): uses existing chla_uncertainty field
      converted to PI90

    Parameters
    ----------
    data_xr : xarray.Dataset
        Combined dataset with all variables (observations, met, static, etc.)
    encoder_vars : list
        Variables to include in encoder input (past observations)
    decoder_vars : list
        Variables to include in decoder input (future forecasts)
    target_vars : list
        Target variables (e.g., ['chla'])
    climatology_xr : xarray.Dataset, optional
        Day-of-year climatology for met variables and PI90 (fallback for decoder)
    gefs_operational_xr : xarray.Dataset, optional
        Operational GEFS forecasts with PI90. When provided, uses actual GEFS
        ensemble PI90 where available, falling back to climatology for historical
        periods. Expected dims: (time, lead_time, site_id, ensemble_member)
    encoder_seq_len : int
        Number of days of historical data for encoder (default 365)
    decoder_seq_len : int
        Number of days of forecast horizon for decoder (default 10)
    spatial_idx_name : str
        Name of spatial coordinate (default 'site_id')
    time_idx_name : str
        Name of time coordinate (default 'time')
    offset : int
        Step size between consecutive samples (default 1 = daily)
    start_date : str, optional
        Start date for sample generation (format: 'YYYY-MM-DD')
    end_date : str, optional
        End date for sample generation (format: 'YYYY-MM-DD')
    include_obs_uncertainty : bool
        Whether to include observation uncertainty (PI90) in output (default True)
    filter_nan_targets : bool
        Whether to filter out samples where all targets are NaN (default True).
        Set to False for test/prediction data where you want forecasts
        regardless of whether observations exist.
    log_transform_target : bool
        Whether to log-transform the target variable (default False).
        When True, applies log(y) transformation which helps with:
        - Equalizing relative errors across concentration range
        - Preventing underprediction of high values
        - Better handling of skewed distributions

    Returns
    -------
    dict : Dictionary containing:
        - 'x_encoder': np.array (n_samples, encoder_seq_len, n_encoder_features)
        - 'x_decoder': np.array (n_samples, decoder_seq_len, n_decoder_features)
        - 'y': np.array (n_samples, decoder_seq_len, n_targets) - log-transformed if log_transform_target=True
        - 'y_obs_pi90': np.array (n_samples, decoder_seq_len, n_targets) - observation uncertainty
        - 'init_dates': np.array of forecast initialization dates
        - 'site_ids': np.array of site IDs for each sample
        - 'target_log_transformed': bool - flag indicating if targets are log-transformed
    """
    # Get full time series (don't slice - we need encoder lookback into training period)
    times = data_xr[time_idx_name].values
    sites = data_xr[spatial_idx_name].values
    n_times = len(times)
    n_sites = len(sites)

    # Calculate valid initialization dates based on data availability
    # Need encoder_seq_len days before and decoder_seq_len days after
    min_idx = encoder_seq_len
    max_idx = n_times - decoder_seq_len

    if max_idx <= min_idx:
        raise ValueError(
            f"Not enough data for encoder_seq_len={encoder_seq_len} and "
            f"decoder_seq_len={decoder_seq_len}. Have {n_times} timesteps."
        )

    # Generate sample indices, then filter by date range
    # This allows encoder to look back beyond start_date into training data
    all_sample_indices = list(range(min_idx, max_idx, offset))

    # Filter sample indices to only include initialization dates within start_date/end_date
    sample_indices = []
    for idx in all_sample_indices:
        init_time = pd.Timestamp(times[idx])
        if start_date is not None and init_time < pd.Timestamp(start_date):
            continue
        if end_date is not None and init_time > pd.Timestamp(end_date):
            continue
        sample_indices.append(idx)

    if len(sample_indices) == 0:
        raise ValueError(
            f"No valid samples found between {start_date} and {end_date}. "
            f"Check that your date range has enough data after accounting for "
            f"encoder_seq_len={encoder_seq_len} days lookback."
        )
    n_samples_per_site = len(sample_indices)
    total_samples = n_samples_per_site * n_sites

    # Pre-allocate arrays
    n_encoder_features = len(encoder_vars)
    n_decoder_features = len(decoder_vars)
    n_targets = len(target_vars)

    x_encoder = np.empty((total_samples, encoder_seq_len, n_encoder_features), dtype=np.float32)
    x_decoder = np.empty((total_samples, decoder_seq_len, n_decoder_features), dtype=np.float32)
    y = np.empty((total_samples, decoder_seq_len, n_targets), dtype=np.float32)
    init_dates = np.empty(total_samples, dtype='datetime64[ns]')
    site_ids = np.empty(total_samples, dtype=object)

    # Pre-allocate observation uncertainty array if requested
    if include_obs_uncertainty:
        y_obs_pi90 = np.empty((total_samples, decoder_seq_len, n_targets), dtype=np.float32)

    # Convert xarray to numpy for faster indexing
    encoder_data = np.stack([data_xr[var].values for var in encoder_vars], axis=-1)
    target_data = np.stack([data_xr[var].values for var in target_vars], axis=-1)

    # Log-transform target if requested
    # This helps with skewed distributions and equalizes relative errors
    if log_transform_target:
        # Use small offset to handle zeros (0.01 µg/L is below detection limits)
        # Negative values (if any) are set to NaN
        target_data = np.where(target_data > 0, target_data, np.nan)
        target_data = np.log(target_data + 0.01)
        print(f"  Applied log-transform to targets: log(chla + 0.01)")

    # Calculate observation uncertainty (PI90) for target variable
    # Using pure proportional error: σ_obs = 0.05 × measurement (constant 5% CV)
    # PI90 = 3.29 × σ_obs
    # Note: Chlorophyll is kept in raw µg/L scale (not log-transformed)
    if include_obs_uncertainty:
        # Get chla values in µg/L
        chla = data_xr[target_vars[0]].values  # Shape: (time, site)

        # Calculate SD: 5% of measurement (constant CV across all concentrations)
        obs_sd = 0.05 * np.abs(chla)

        # Convert SD to PI90: PI90 = 3.29 × σ (for Gaussian, Q95 - Q05)
        obs_pi90 = 3.29 * obs_sd

        # Stack for multiple targets (currently just chla)
        target_obs_pi90 = np.stack([obs_pi90], axis=-1)  # Shape: (time, site, n_targets)

    # Identify met vars, PI90 vars, and static vars in decoder
    # PI90 vars are those ending with '_pi90'
    pi90_vars = [v for v in decoder_vars if v.endswith('_pi90')]
    base_met_vars = [v.replace('_pi90', '') for v in pi90_vars]

    # Met vars are the base meteorological variables (matching those with PI90)
    met_vars = [v for v in decoder_vars if v in base_met_vars]

    # Static vars are everything else (lat, lon, land cover, etc.)
    # These are time-invariant features that are safe to pull from any time index
    static_vars = [v for v in decoder_vars if v not in met_vars and v not in pi90_vars]

    # Build decoder data with blending if operational GEFS is available
    if gefs_operational_xr is not None and climatology_xr is not None:
        print("  Building decoder with blended GEFS/climatology PI90...")

        # Get sample init times (times at filtered sample indices)
        sample_times = times[sample_indices]

        # Build blended PI90 data
        pi90_data = build_decoder_pi90_blended(
            times=sample_times,
            sites=sites,
            met_vars=base_met_vars,
            gefs_operational_xr=gefs_operational_xr,
            climatology_xr=climatology_xr,
            decoder_seq_len=decoder_seq_len
        )

        # Build blended met forecast data
        met_data = build_decoder_met_blended(
            times=sample_times,
            sites=sites,
            met_vars=met_vars,  # met_vars now correctly contains only meteorological variables
            gefs_operational_xr=gefs_operational_xr,
            climatology_xr=climatology_xr,
            decoder_seq_len=decoder_seq_len
        )

        # Get static features (same for all lead times)
        # Static features have been broadcast to (time, site_id) but are constant across time
        # Extract just one time slice to get shape (n_sites,)
        static_data = {}
        for var in static_vars:
            if var in data_xr.data_vars:
                if 'time' in data_xr[var].dims:
                    # Take first time point since values are constant across time
                    static_data[var] = data_xr[var].isel(time=0).values
                else:
                    static_data[var] = data_xr[var].values

        # Generate samples with blended decoder data
        sample_idx = 0
        for site_idx, site in enumerate(sites):
            for sample_t_idx, t_idx in enumerate(sample_indices):
                # Encoder: past encoder_seq_len days
                encoder_start = t_idx - encoder_seq_len
                encoder_end = t_idx
                x_encoder[sample_idx] = encoder_data[encoder_start:encoder_end, site_idx, :]

                # Decoder: build from blended data
                for feat_idx, var in enumerate(decoder_vars):
                    if var in ['chla_lagged', 'chla_uncertainty_lagged']:
                        # Special handling for autoregressive features:
                        # - Day 0: use actual lagged value (observation at t-1)
                        # - Days 1+: initialize with 0 (will be replaced by model predictions during forward_autoregressive)
                        # This avoids NaN issues and makes the autoregressive intent explicit
                        lagged_day0 = data_xr[var].values[t_idx, site_idx] if var in data_xr else 0.0
                        x_decoder[sample_idx, 0, feat_idx] = lagged_day0 if not np.isnan(lagged_day0) else 0.0
                        x_decoder[sample_idx, 1:, feat_idx] = 0.0  # Placeholder for autoregressive rollout
                    elif var in pi90_data:
                        # PI90 from blended data (already per-sample, per-site, per-lead)
                        x_decoder[sample_idx, :, feat_idx] = pi90_data[var][sample_t_idx, site_idx, :]
                    elif var in met_data:
                        # Met forecast from blended data
                        x_decoder[sample_idx, :, feat_idx] = met_data[var][sample_t_idx, site_idx, :]
                    elif var in static_data:
                        # Static features (same for all lead times)
                        x_decoder[sample_idx, :, feat_idx] = static_data[var][site_idx]
                    else:
                        # WARNING: This fallback uses data from the FORECAST PERIOD
                        # Only safe for time-invariant variables!
                        # If you see this warning for time-varying variables, there may be data leakage.
                        if sample_idx == 0:
                            import warnings
                            warnings.warn(
                                f"Decoder variable '{var}' not found in pi90_data, met_data, or static_data. "
                                f"Falling back to data_xr values from forecast period. "
                                f"This is only safe for time-invariant (static) features!"
                            )
                        decoder_start = t_idx
                        decoder_end = t_idx + decoder_seq_len
                        x_decoder[sample_idx, :, feat_idx] = data_xr[var].values[decoder_start:decoder_end, site_idx]

                # Target: actual observations for decoder period
                decoder_start = t_idx
                decoder_end = t_idx + decoder_seq_len
                y[sample_idx] = target_data[decoder_start:decoder_end, site_idx, :]

                # Observation uncertainty (PI90) for target
                if include_obs_uncertainty:
                    y_obs_pi90[sample_idx] = target_obs_pi90[decoder_start:decoder_end, site_idx, :]

                # Metadata
                init_dates[sample_idx] = times[t_idx]
                site_ids[sample_idx] = site

                sample_idx += 1
    else:
        # Fall back to original behavior (climatology only or direct data)
        if climatology_xr is not None:
            decoder_data = _build_decoder_from_climatology(
                data_xr, decoder_vars, climatology_xr, times
            )
        else:
            # WARNING: This path uses OBSERVED data for decoder inputs!
            # This causes DATA LEAKAGE unless all decoder_vars are time-invariant.
            import warnings
            warnings.warn(
                "No climatology_xr provided - using observed data for decoder inputs. "
                "This causes DATA LEAKAGE for time-varying variables! "
                "Only use this path if all decoder_vars are static features."
            )
            decoder_data = np.stack([data_xr[var].values for var in decoder_vars], axis=-1)

        # Generate samples
        sample_idx = 0
        for site_idx, site in enumerate(sites):
            for t_idx in sample_indices:
                # Encoder: past encoder_seq_len days
                encoder_start = t_idx - encoder_seq_len
                encoder_end = t_idx
                x_encoder[sample_idx] = encoder_data[encoder_start:encoder_end, site_idx, :]

                # Decoder: future decoder_seq_len days
                decoder_start = t_idx
                decoder_end = t_idx + decoder_seq_len
                x_decoder[sample_idx] = decoder_data[decoder_start:decoder_end, site_idx, :]

                # Special handling for chla_lagged in decoder (autoregressive mode)
                # Day 0: use actual lagged chla from encoder data
                # Days 1+: fill with 0 (placeholder for autoregressive rollout during training)
                for var in ['chla_lagged', 'chla_uncertainty_lagged']:
                    if var in decoder_vars:
                        feat_idx = decoder_vars.index(var)
                        # Get actual lagged value from encoder data (last timestep of encoder)
                        if var in encoder_vars:
                            enc_feat_idx = encoder_vars.index(var)
                            lagged_day0 = encoder_data[encoder_end - 1, site_idx, enc_feat_idx]
                        elif var in data_xr:
                            lagged_day0 = data_xr[var].values[t_idx, site_idx]
                        else:
                            lagged_day0 = 0.0
                        x_decoder[sample_idx, 0, feat_idx] = lagged_day0 if not np.isnan(lagged_day0) else 0.0
                        x_decoder[sample_idx, 1:, feat_idx] = 0.0  # Placeholder for autoregressive

                # Target: actual observations for decoder period
                y[sample_idx] = target_data[decoder_start:decoder_end, site_idx, :]

                # Observation uncertainty (PI90) for target
                if include_obs_uncertainty:
                    y_obs_pi90[sample_idx] = target_obs_pi90[decoder_start:decoder_end, site_idx, :]

                # Metadata
                init_dates[sample_idx] = times[t_idx]
                site_ids[sample_idx] = site

                sample_idx += 1

    # Filter out samples where ALL targets are NaN (no useful training signal)
    # Keep samples with at least one valid observation in the target sequence
    # Skip filtering if filter_nan_targets=False (e.g., for test/prediction data)
    if filter_nan_targets:
        valid_mask = ~np.all(np.isnan(y), axis=(1, 2))
        n_valid = valid_mask.sum()
        n_total = len(valid_mask)
        n_removed = n_total - n_valid
        if n_removed > 0:
            print(f"  Filtered {n_removed} samples with all-NaN targets ({100*n_removed/n_total:.1f}%)")
            print(f"  Keeping {n_valid} samples with at least one valid observation")
            x_encoder = x_encoder[valid_mask]
            x_decoder = x_decoder[valid_mask]
            y = y[valid_mask]
            init_dates = init_dates[valid_mask]
            site_ids = site_ids[valid_mask]
            if include_obs_uncertainty:
                y_obs_pi90 = y_obs_pi90[valid_mask]

    # Build return dictionary
    result = {
        'x_encoder': x_encoder,
        'x_decoder': x_decoder,
        'y': y,
        'init_dates': init_dates,
        'site_ids': site_ids,
        'encoder_vars': np.array(encoder_vars),
        'decoder_vars': np.array(decoder_vars),
        'target_vars': np.array(target_vars),
        'target_log_transformed': log_transform_target
    }

    # Add observation uncertainty if requested
    if include_obs_uncertainty:
        result['y_obs_pi90'] = y_obs_pi90

    return result


def _build_decoder_from_climatology(data_xr, decoder_vars, climatology_xr, times):
    """
    Build decoder input array using climatology values.

    For each timestep, maps the day-of-year to climatological values.
    This provides decoder inputs with high uncertainty (PI90) for historical
    training periods without operational forecasts.

    Parameters
    ----------
    data_xr : xarray.Dataset
        Dataset with static features and structure
    decoder_vars : list
        Variables to include in decoder
    climatology_xr : xarray.Dataset
        Day-of-year climatology with means and PI90
    times : np.array
        Time values from data_xr

    Returns
    -------
    decoder_data : np.array
        Shape (n_times, n_sites, n_decoder_features)
    """
    n_times = len(times)
    n_sites = len(data_xr['site_id'])
    n_features = len(decoder_vars)

    decoder_data = np.empty((n_times, n_sites, n_features), dtype=np.float32)

    # Get day of year for each time
    doys = pd.DatetimeIndex(times).dayofyear

    for feat_idx, var in enumerate(decoder_vars):
        if var in ['chla_lagged', 'chla_uncertainty_lagged']:
            # Special handling for autoregressive features
            # Fill with 0 as placeholder - actual values are set in sample loop
            decoder_data[:, :, feat_idx] = 0.0
        elif var in climatology_xr.data_vars:
            # Use climatology (maps DOY to value)
            clim_values = climatology_xr[var].values  # Shape: (366, n_sites) or (366,)
            if clim_values.ndim == 1:
                # Broadcast to all sites
                for t_idx, doy in enumerate(doys):
                    decoder_data[t_idx, :, feat_idx] = clim_values[doy - 1]  # DOY is 1-indexed
            else:
                for t_idx, doy in enumerate(doys):
                    decoder_data[t_idx, :, feat_idx] = clim_values[doy - 1, :]
        elif var in data_xr.data_vars:
            # Use actual data - should only be static features!
            # WARNING: This is only safe for time-invariant features
            import warnings
            warnings.warn(
                f"Decoder variable '{var}' not in climatology, using data_xr values. "
                f"This is only safe for time-invariant (static) features!"
            )
            decoder_data[:, :, feat_idx] = data_xr[var].values
        else:
            # Variable not found, fill with NaN
            decoder_data[:, :, feat_idx] = np.nan

    return decoder_data


def create_encoder_decoder_batches(
    encoder_decoder_dict,
    batch_size=None
):
    """
    Convert encoder-decoder samples into training batches.

    Parameters
    ----------
    encoder_decoder_dict : dict
        Output from create_encoder_decoder_samples()
    batch_size : int, optional
        If provided, split into mini-batches. Otherwise return full arrays.

    Returns
    -------
    dict : Dictionary with batched arrays ready for training
    """
    x_enc = encoder_decoder_dict['x_encoder']
    x_dec = encoder_decoder_dict['x_decoder']
    y = encoder_decoder_dict['y']

    if batch_size is None:
        return {
            'x_encoder': x_enc,
            'x_decoder': x_dec,
            'y': y,
            'encoder_vars': encoder_decoder_dict['encoder_vars'],
            'decoder_vars': encoder_decoder_dict['decoder_vars'],
            'target_vars': encoder_decoder_dict['target_vars'],
            'init_dates': encoder_decoder_dict['init_dates'],
            'site_ids': encoder_decoder_dict['site_ids']
        }

    # Shuffle and batch
    n_samples = x_enc.shape[0]
    indices = np.random.permutation(n_samples)

    n_batches = (n_samples + batch_size - 1) // batch_size
    batches = []

    for i in range(n_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, n_samples)
        batch_indices = indices[start_idx:end_idx]

        batches.append({
            'x_encoder': x_enc[batch_indices],
            'x_decoder': x_dec[batch_indices],
            'y': y[batch_indices],
            'init_dates': encoder_decoder_dict['init_dates'][batch_indices],
            'site_ids': encoder_decoder_dict['site_ids'][batch_indices]
        })

    return batches


def scale_encoder_decoder(
    encoder_decoder_dict,
    encoder_std=None,
    encoder_mean=None,
    decoder_std=None,
    decoder_mean=None,
    target_std=None,
    target_mean=None
):
    """
    Scale encoder, decoder, and target arrays.

    If std/mean not provided, computes from the data.

    Parameters
    ----------
    encoder_decoder_dict : dict
        Output from create_encoder_decoder_samples()
    encoder_std, encoder_mean : np.array, optional
        Scaling parameters for encoder (shape: n_encoder_features,)
    decoder_std, decoder_mean : np.array, optional
        Scaling parameters for decoder (shape: n_decoder_features,)
    target_std, target_mean : np.array, optional
        Scaling parameters for target (shape: n_targets,)

    Returns
    -------
    dict : Scaled encoder_decoder_dict with added scaling parameters
    """
    result = encoder_decoder_dict.copy()

    # Scale encoder
    x_enc = encoder_decoder_dict['x_encoder']
    if encoder_std is None or encoder_mean is None:
        # Compute from data (flatten samples and time)
        flat_enc = x_enc.reshape(-1, x_enc.shape[-1])

        # Suppress warnings for features with all NaN (e.g. total_cloud_cover_atmosphere
        # from the FLARE S3 met sources, which have no equivalent variable)
        with np.errstate(all='ignore'):
            encoder_mean = np.nanmean(flat_enc, axis=0)
            encoder_std = np.nanstd(flat_enc, axis=0) + 1e-10

        # Handle features with all NaN values (use neutral scaling: mean=0, std=1)
        nan_mean_mask = np.isnan(encoder_mean)
        nan_std_mask = np.isnan(encoder_std) | (encoder_std < 1e-9)
        if np.any(nan_mean_mask):
            print(f"  Note: {np.sum(nan_mean_mask)} encoder features have all NaN values (using neutral scaling)")
            encoder_mean[nan_mean_mask] = 0.0
            encoder_std[nan_std_mask] = 1.0

    result['x_encoder'] = (x_enc - encoder_mean) / encoder_std
    result['encoder_mean'] = encoder_mean
    result['encoder_std'] = encoder_std

    # Features that are entirely NaN (e.g. total_cloud_cover_atmosphere from the
    # FLARE S3 met sources, which have no equivalent variable) are neutrally-scaled
    # above (mean=0, std=1), but the values themselves are still NaN and would
    # propagate NaN through the encoder forward pass / loss. Unlike the decoder,
    # no encoder feature is overwritten per-timestep during the forward pass, so
    # fill all remaining NaN encoder values with 0 (the neutral scaled value).
    enc_still_nan = np.isnan(result['x_encoder'])
    if np.any(enc_still_nan):
        print(f"  Note: filling {enc_still_nan.sum()} remaining NaN encoder values with 0 (neutral scaled value)")
        result['x_encoder'] = np.where(enc_still_nan, 0.0, result['x_encoder'])

    # Scale decoder
    x_dec = encoder_decoder_dict['x_decoder']
    if decoder_std is None or decoder_mean is None:
        flat_dec = x_dec.reshape(-1, x_dec.shape[-1])

        # Suppress warnings for features with all NaN (e.g., chla_lagged for future decoder days)
        with np.errstate(all='ignore'):
            decoder_mean = np.nanmean(flat_dec, axis=0)
            decoder_std = np.nanstd(flat_dec, axis=0) + 1e-10

        # Handle features with all NaN values (use neutral scaling: mean=0, std=1)
        # This is fine for autoregressive features (chla_lagged) which get replaced during forward pass
        nan_mean_mask = np.isnan(decoder_mean)
        nan_std_mask = np.isnan(decoder_std) | (decoder_std < 1e-9)
        if np.any(nan_mean_mask):
            print(f"  Note: {np.sum(nan_mean_mask)} decoder features have all NaN values (using neutral scaling)")
            decoder_mean[nan_mean_mask] = 0.0
            decoder_std[nan_std_mask] = 1.0

    result['x_decoder'] = (x_dec - decoder_mean) / decoder_std
    result['decoder_mean'] = decoder_mean
    result['decoder_std'] = decoder_std

    # Features that are entirely NaN (e.g. total_cloud_cover_atmosphere from the
    # FLARE S3 met sources, which have no equivalent variable) are neutrally-scaled
    # above (mean=0, std=1), but the values themselves are still NaN and would
    # propagate NaN through the whole forward pass / loss. chla_lagged/
    # chla_uncertainty_lagged are the one exception: their NaN placeholders (for
    # lead times > 0) are intentionally overwritten per-timestep by the
    # autoregressive forward pass, so leave those as NaN here.
    decoder_vars = encoder_decoder_dict.get('decoder_vars', [])
    autoregressive_vars = {'chla_lagged', 'chla_uncertainty_lagged'}
    fillable_mask = np.array([v not in autoregressive_vars for v in decoder_vars], dtype=bool)
    still_nan = np.isnan(result['x_decoder'])
    if fillable_mask.size:
        still_nan &= fillable_mask[np.newaxis, np.newaxis, :]
    if np.any(still_nan):
        print(f"  Note: filling {still_nan.sum()} remaining NaN decoder values with 0 (neutral scaled value)")
        result['x_decoder'] = np.where(still_nan, 0.0, result['x_decoder'])

    # Note: Target (y) typically not scaled for CMAL loss
    # but include scaling params if needed
    y = encoder_decoder_dict['y']
    if target_std is None or target_mean is None:
        flat_y = y.reshape(-1, y.shape[-1])
        target_mean = np.nanmean(flat_y, axis=0)
        target_std = np.nanstd(flat_y, axis=0) + 1e-10

    result['target_mean'] = target_mean
    result['target_std'] = target_std
    # Keep y unscaled (CMAL loss uses raw values)
    result['y'] = y

    # Preserve observation uncertainty (PI90) if present
    # Note: y_obs_pi90 is NOT scaled - it represents the uncertainty width in µg/L
    # and should not be z-scored (it's used directly in the loss function)
    if 'y_obs_pi90' in encoder_decoder_dict:
        result['y_obs_pi90'] = encoder_decoder_dict['y_obs_pi90']

    # Preserve log-transform flag
    if 'target_log_transformed' in encoder_decoder_dict:
        result['target_log_transformed'] = encoder_decoder_dict['target_log_transformed']

    return result


def prepare_decoder_with_forecast(
    data_xr,
    forecast_xr,
    decoder_vars,
    climatology_xr,
    operational_start_date,
    time_idx_name="time"
):
    """
    Prepare decoder inputs using operational forecasts where available,
    falling back to climatology for historical periods.

    Parameters
    ----------
    data_xr : xarray.Dataset
        Base dataset with structure
    forecast_xr : xarray.Dataset
        Operational GEFS forecasts with ensemble_member dimension
    decoder_vars : list
        Variables to include in decoder
    climatology_xr : xarray.Dataset
        Day-of-year climatology for fallback
    operational_start_date : str
        Date from which operational forecasts are available (format: 'YYYY-MM-DD')
    time_idx_name : str
        Name of time coordinate

    Returns
    -------
    decoder_data : xarray.Dataset
        Decoder inputs with dims (time, lead_time, site_id)
    """
    op_start = np.datetime64(operational_start_date)
    times = data_xr[time_idx_name].values

    # Separate into pre-operational and operational periods
    pre_op_mask = times < op_start
    op_mask = times >= op_start

    # For pre-operational period: use climatology
    # For operational period: use GEFS ensemble median

    # This is a placeholder - full implementation would require
    # more careful handling of the lead_time dimension
    # For now, return climatology-based decoder
    return _build_decoder_from_climatology(
        data_xr, decoder_vars, climatology_xr, times
    )


def build_decoder_pi90_blended(
    times,
    sites,
    met_vars,
    gefs_operational_xr,
    climatology_xr,
    decoder_seq_len=10
):
    """
    Build decoder PI90 arrays by blending GEFS ensemble PI90 (where available)
    with climatology PI90 fallback (for historical periods).

    For dates with operational GEFS forecasts:
        - Uses actual ensemble spread (Q95 - Q05) which is typically 4-12
        - This signals "trust this forecast" to the model

    For historical dates without operational forecasts:
        - Uses climatology PI90 (4×std) which is typically 15-20
        - This signals "don't trust this, rely on encoder state"

    Parameters
    ----------
    times : np.array
        Array of forecast initialization times (datetime64)
    sites : np.array
        Array of site IDs
    met_vars : list
        Meteorological variable names (without _pi90 suffix)
    gefs_operational_xr : xarray.Dataset
        Operational GEFS forecasts with PI90 already computed.
        Expected dims: (time, lead_time, site_id) for PI90 variables
    climatology_xr : xarray.Dataset
        Day-of-year climatology with PI90 (dayofyear, site_id)
    decoder_seq_len : int
        Number of forecast days (default 10)

    Returns
    -------
    pi90_data : dict
        Dictionary mapping f"{var}_pi90" to np.array with shape
        (n_times, n_sites, decoder_seq_len)
    """
    n_times = len(times)
    n_sites = len(sites)

    # Determine which times have operational GEFS available
    if gefs_operational_xr is not None and 'time' in gefs_operational_xr.dims:
        op_times = gefs_operational_xr['time'].values
        op_time_min = op_times.min()
        op_time_max = op_times.max()
    else:
        op_time_min = np.datetime64('2100-01-01')  # Far future = no operational data
        op_time_max = np.datetime64('2100-01-01')

    pi90_data = {}

    for var in met_vars:
        pi90_var = f"{var}_pi90"
        pi90_array = np.empty((n_times, n_sites, decoder_seq_len), dtype=np.float32)

        # Get climatology PI90 values (indexed by day-of-year)
        if pi90_var in climatology_xr.data_vars:
            clim_pi90 = climatology_xr[pi90_var].values  # Shape: (366,) or (366, n_sites)
        else:
            # Fallback: use a large constant if climatology not available
            clim_pi90 = np.full(366, 20.0, dtype=np.float32)

        for t_idx, init_time in enumerate(times):
            # Check if this time has operational GEFS data
            has_operational = (init_time >= op_time_min) and (init_time <= op_time_max)

            if has_operational and gefs_operational_xr is not None:
                try:
                    # Try to get GEFS ensemble PI90 for this init time
                    gefs_pi90 = gefs_operational_xr[pi90_var].sel(
                        time=init_time, method='nearest'
                    )
                    # Get values for all sites and lead times
                    # Shape should be (lead_time, site_id) after selection
                    gefs_pi90_values = gefs_pi90.values

                    # Handle dimension ordering - we want (site_id, lead_time)
                    if gefs_pi90_values.ndim == 2:
                        # Transpose if needed to get (lead_time, site_id)
                        if gefs_pi90.dims[0] == 'site_id':
                            gefs_pi90_values = gefs_pi90_values.T

                        # Truncate or pad to decoder_seq_len
                        n_lead = gefs_pi90_values.shape[0]
                        if n_lead >= decoder_seq_len:
                            pi90_array[t_idx, :, :] = gefs_pi90_values[:decoder_seq_len, :].T
                        else:
                            # Pad with last value if GEFS doesn't cover full horizon
                            pi90_array[t_idx, :, :n_lead] = gefs_pi90_values.T
                            pi90_array[t_idx, :, n_lead:] = gefs_pi90_values[-1, :, np.newaxis]
                    else:
                        # Unexpected shape, fall back to climatology
                        raise ValueError(f"Unexpected PI90 shape: {gefs_pi90_values.shape}")

                except (KeyError, ValueError) as e:
                    # Fall back to climatology if GEFS selection fails
                    has_operational = False

            if not has_operational:
                # Use climatology PI90 for each day in the decoder sequence
                # lead_day=0 corresponds to init_time (first prediction day)
                for lead_day in range(decoder_seq_len):
                    forecast_date = init_time + np.timedelta64(lead_day, 'D')
                    doy = pd.Timestamp(forecast_date).dayofyear

                    if clim_pi90.ndim == 1:
                        pi90_array[t_idx, :, lead_day] = clim_pi90[doy - 1]
                    else:
                        pi90_array[t_idx, :, lead_day] = clim_pi90[doy - 1, :]

        pi90_data[pi90_var] = pi90_array

    return pi90_data


def build_decoder_met_blended(
    times,
    sites,
    met_vars,
    gefs_operational_xr,
    climatology_xr,
    decoder_seq_len=10
):
    """
    Build decoder meteorological forecast arrays by blending GEFS ensemble median
    (where available) with climatology mean fallback (for historical periods).

    Parameters
    ----------
    times : np.array
        Array of forecast initialization times (datetime64)
    sites : np.array
        Array of site IDs
    met_vars : list
        Meteorological variable names
    gefs_operational_xr : xarray.Dataset
        Operational GEFS forecasts (should already have ensemble collapsed to median)
    climatology_xr : xarray.Dataset
        Day-of-year climatology means
    decoder_seq_len : int
        Number of forecast days (default 10)

    Returns
    -------
    met_data : dict
        Dictionary mapping var to np.array with shape (n_times, n_sites, decoder_seq_len)
    """
    n_times = len(times)
    n_sites = len(sites)

    # Determine which times have operational GEFS available
    if gefs_operational_xr is not None and 'time' in gefs_operational_xr.dims:
        op_times = gefs_operational_xr['time'].values
        op_time_min = op_times.min()
        op_time_max = op_times.max()
    else:
        op_time_min = np.datetime64('2100-01-01')
        op_time_max = np.datetime64('2100-01-01')

    met_data = {}

    for var in met_vars:
        met_array = np.empty((n_times, n_sites, decoder_seq_len), dtype=np.float32)

        # Get climatology values (indexed by day-of-year)
        if var in climatology_xr.data_vars:
            clim_values = climatology_xr[var].values
        else:
            clim_values = np.zeros(366, dtype=np.float32)

        for t_idx, init_time in enumerate(times):
            has_operational = (init_time >= op_time_min) and (init_time <= op_time_max)

            if has_operational and gefs_operational_xr is not None:
                try:
                    # Get GEFS forecast for this init time
                    # First collapse ensemble to median if still present
                    if 'ensemble_member' in gefs_operational_xr.dims:
                        gefs_var = gefs_operational_xr[var].sel(
                            time=init_time, method='nearest'
                        ).quantile(0.5, dim='ensemble_member')
                    else:
                        gefs_var = gefs_operational_xr[var].sel(
                            time=init_time, method='nearest'
                        )

                    gefs_values = gefs_var.values

                    if gefs_values.ndim == 2:
                        if gefs_var.dims[0] == 'site_id':
                            gefs_values = gefs_values.T

                        n_lead = gefs_values.shape[0]
                        if n_lead >= decoder_seq_len:
                            met_array[t_idx, :, :] = gefs_values[:decoder_seq_len, :].T
                        else:
                            met_array[t_idx, :, :n_lead] = gefs_values.T
                            met_array[t_idx, :, n_lead:] = gefs_values[-1, :, np.newaxis]
                    else:
                        raise ValueError(f"Unexpected shape: {gefs_values.shape}")

                except (KeyError, ValueError):
                    has_operational = False

            if not has_operational:
                # lead_day=0 corresponds to init_time (first prediction day)
                for lead_day in range(decoder_seq_len):
                    forecast_date = init_time + np.timedelta64(lead_day, 'D')
                    doy = pd.Timestamp(forecast_date).dayofyear

                    if clim_values.ndim == 1:
                        met_array[t_idx, :, lead_day] = clim_values[doy - 1]
                    else:
                        met_array[t_idx, :, lead_day] = clim_values[doy - 1, :]

        met_data[var] = met_array

    return met_data
