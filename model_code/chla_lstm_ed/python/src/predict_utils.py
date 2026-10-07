import pandas as pd
import numpy as np
import xarray as xr
import datetime
import torch
from numpy.lib.npyio import NpzFile

def get_data_if_file(d):
    """
    rudimentary check if data .npz file is already loaded. if not, load it
    :param d:
    :return:
    """
    if isinstance(d, NpzFile) or isinstance(d, dict):
        return d
    else:
        return np.load(d, allow_pickle=True)


def unscale_output(y_scl, y_std, y_mean, y_vars, log_vars=None):
    """
    unscale output data given a standard deviation and a mean value for the
    outputs
    :param y_scl: [pd dataframe] scaled output data (predicted or observed)
    :param y_std:[numpy array] array of standard deviation of variables_to_log [n_out]
    :param y_mean:[numpy array] array of variable means [n_out]
    :param y_vars: [list-like] y_dataset variable names
    :param log_vars: [list-like] which variables_to_log (if any) were logged in data
    prep
    :return: unscaled data
    """
    y_unscaled = y_scl.copy()
    # I'm replacing just the variable columns. I have to specify because, at
    # least in some cases, there are other columns (e.g., "seg_id_nat" and
    # date")
    y_unscaled[y_vars] = (y_scl[y_vars] * y_std) + y_mean
    if log_vars:
        y_unscaled[log_vars] = np.exp(y_unscaled[log_vars])
    return y_unscaled



def predict_from_io_data(
    model,
    head, 
    io_data,
    partition,
    outfile,
    log_vars=False,
    trn_offset = 1.0,
    tst_val_offset = 1.0,
    spatial_idx_name="site_id",
    time_idx_name="time",
    trn_latest_time=None,
    val_latest_time=None,
    tst_latest_time=None,
    all_dates_latest_time=None
):
    """
    make predictions from trained model
    :param io_data: [str] directory to prepped data file
    :param partition: [str] must be 'trn' or 'tst'; whether you want to predict
    for the train or the dev period
    :param outfile: [str] the file where the output data should be stored
    :param log_vars: [list-like] which variables_to_log (if any) were logged in data
    prep
    :param trn_offset: [str] value for the training offset
    :param tst_val_offset: [str] value for the testing and validation offset
    :param trn_latest_time: [str] when specified, the training partition preds will
    be trimmed to use trn_latest_time as the last date
    :param val_latest_time: [str] when specified, the validation partition preds will
    be trimmed to use val_latest_time as the last date
    :param tst_latest_time: [str] when specified, the test partition preds will
    be trimmed to use tst_latest_time as the last date
    :return: [pd dataframe] predictions
    """
    io_data = get_data_if_file(io_data)
    if partition == "train":
        keep_portion = trn_offset
        if trn_latest_time:
            latest_time = trn_latest_time
        else:
            latest_time = None
    elif partition == "val":
        keep_portion = tst_val_offset
        if val_latest_time:
            latest_time = val_latest_time
        else:
            latest_time = None
    elif partition == "test":
        keep_portion = tst_val_offset
        if tst_latest_time:
            latest_time = tst_latest_time
        else:
            latest_time = None
    elif partition == "all_dates":
        keep_portion = tst_val_offset
        if all_dates_latest_time:
            latest_time = all_dates_latest_time
        else:
            latest_time = None
    
    target_log_transformed = bool(io_data.get('target_log_transformed', False))

    preds = predict(
        model,
        head,
        io_data[f"x_{partition}"],
        io_data[f"ids_{partition}"],
        io_data[f"times_{partition}"],
        io_data["obs_std"],
        io_data["obs_mean"],
        io_data["obs_vars"],
        io_data[f"h_{partition}"],
        io_data[f"c_{partition}"],
        io_data[f"weighting_matrix_{partition}"],
        keep_last_portion=keep_portion,
        outfile=outfile,
        log_vars=log_vars,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
        latest_time=latest_time,
        pad_mask=io_data[f"padded_{partition}"],
        log_transform=target_log_transformed,
    )
    return preds


def predict(
    model,
    head,
    x_data,
    pred_ids,
    pred_dates,
    y_stds,
    y_means,
    y_vars,
    h,
    c,
    weighting_matrix,
    keep_last_portion=1.0,
    outfile=None,
    log_vars=False,
    spatial_idx_name="site_id",
    time_idx_name="time",
    latest_time=None,
    pad_mask=None,
    log_transform=False,
):
    """
    use trained model to make predictions
    :param model: [tf model] trained TF model to use for predictions
    :param x_data: [np array] numpy array of scaled and centered x_data
    :param pred_ids: [np array] the ids of the segments (same shape as x_data)
    :param pred_dates: [np array] the dates of the segments (same shape as
    x_data)
    :param keep_last_portion: [float] fraction of the predictions to keep starting
    from the *end* of the predictions (0-1). (1 means you keep all of the
    predictions, .75 means you keep the final three quarters of the predictions). Alternatively, if
    keep_last_portion is > 1 it's taken as an absolute number of predictions to retain from the end of the
    prediction sequence.
    :param y_stds:[np array] the standard deviation of the y_dataset data
    :param y_means:[np array] the means of the y_dataset data
    :param y_vars:[np array] the variable names of the y_dataset data
    :param outfile: [str] the file where the output data should be stored
    :param log_vars: [list-like] which variables_to_log (if any) were logged in data
    :param latest_time: [str] when provided, the latest time that should be included
    in the returned dataframe
    :param pad_mask: [np array] bool array with True for padded data and False
    otherwise
    :return: out predictions
    """
    num_segs = len(np.unique(pred_ids))
    if head == "GMM":
        y_mu, y_sigma = predict_torch(x_data, model, head, batch_size=x_data.shape[0],
                                h = h, c = c, weighting_matrix = weighting_matrix)
    if head == "CMAL":
        y_mu, y_b, y_tau = predict_torch(x_data, model, head, batch_size=x_data.shape[0],
                                h = h, c = c, weighting_matrix = weighting_matrix)
    else: 
        raise ValueError("Head must be GMM or CMAL")
    
    # keep only specified part of predictions
    if keep_last_portion>1:
        frac_seq_len = int(keep_last_portion)
    else:
        frac_seq_len = round(pred_ids.shape[1] * (keep_last_portion))

    if head == "GMM":
        y_mu = y_mu[:, -frac_seq_len:,...]
        y_sigma = y_sigma[:, -frac_seq_len:,...] 
    if head == "CMAL":
        y_mu = y_mu[:, -frac_seq_len:,...] 
        y_b = y_b[:, -frac_seq_len:,...] 
        y_tau = y_tau[:, -frac_seq_len:,...] 
    #set to nan the data that were added to fill batches
    if pad_mask is not None:
        pad_mask = torch.tensor(pad_mask[:, -frac_seq_len:,...])
        if head == "GMM":
            y_mu[pad_mask] = np.nan
            y_sigma[pad_mask] = np.nan 
        if head == "CMAL":
            y_mu[pad_mask] = np.nan
            y_b[pad_mask] = np.nan
            y_tau[pad_mask] = np.nan
    pred_ids = pred_ids[:, -frac_seq_len:,...]
    pred_dates = pred_dates[:, -frac_seq_len:,...]

    if head == "GMM":
        y_pred_pp = prepped_array_to_df((y_mu, y_sigma), head, pred_dates, pred_ids, y_vars, spatial_idx_name, time_idx_name, log_transform=log_transform)
    if head == "CMAL":
        y_pred_pp = prepped_array_to_df((y_mu, y_b, y_tau), head, pred_dates, pred_ids, y_vars, spatial_idx_name, time_idx_name, log_transform=log_transform)
    
    # TODO: unscale 
    # y_pred_pp = unscale_output(y_pred_pp, y_stds, y_means, y_vars, log_vars)
    
    #remove data that were added to fill batches
    y_pred_pp.dropna(subset=['prediction'], inplace=True)
    y_pred_pp = y_pred_pp.reset_index().drop(columns='index')

    #Cut off the end times if specified
    if latest_time:
        y_pred_pp = (y_pred_pp.drop(y_pred_pp[y_pred_pp['datetime'] > np.datetime64(latest_time)].index)
                     .reset_index()
                     .drop(columns='index')
                     )

    if outfile:
        y_pred_pp.to_feather(outfile)
    return y_pred_pp


def mean_or_std_dataset_from_np(data, data_label, var_names_label):
    """
    turn a numpy data array of means or standard deviations into a xarry dataset
    :param data: the numpy NpzFile
    :param data_label: [str] the label for the values you want to turn into the
    xarray dataset (i.e., "x_mean" or "x_std")
    :param var_names_label: [str] the label of the data you want to become the
    variable names of the xarray dataset (i.e., "x_cols")
    :return:xarray dataset of the means or standard deviations
    """
    df = pd.DataFrame([data[data_label]], columns=data[var_names_label])
    # take the "min" to drop index level. it's only one value per variable
    # so the minis meaningless
    ds = df.to_xarray().min()
    return ds


def swap_first_seq_halves(x_data, batch_size):
    """
    make an additional batch from the first batch. the additional batch will
    have the first and second halves of the original first batch switched

    :param x_data: [np array] x data with shape [nseg * nbatch, seq_len, nfeat]
    :param batch_size: [int] the size of the batch (number of segments)
    :return: [np array] original data with an additional batch
    """
    first_batch = x_data[:batch_size, :, :]
    seq_len = x_data.shape[1]
    half_size = round(seq_len / 2)
    first_half_first_batch = first_batch[:, :half_size, :]
    second_half_first_batch = first_batch[:, half_size:, :]
    swapped = np.concatenate(
        [second_half_first_batch, first_half_first_batch], axis=1
    )
    new_x_data = np.concatenate([swapped, x_data], axis=0)
    return new_x_data


def predict_one_date_range(
    model,
    ds_x_scaled,
    train_io_data,
    seq_len,
    start_date,
    end_date,
    spatial_idx_name="site_id",
    time_idx_name="time",
    log_vars=None,
    keep_last_frac=1.0,
    offset=0.5,
    swap_halves_of_first_seq=False,
):
    """
    make predictions for one date range. This was broken out to be able to do
    the "beginning - middle - end" predictions more easily

    :param model: loaded tensorflow model
    :param ds_x_scaled: [xr array] scaled x data
    :param train_io_data: [np NpzFile] data containing the y_std, y_mean, y_vars
    :param seq_len: [int] length of the prediction sequences (usu. 365)
    :param start_date: [str or date] the start date of the predictions
    :param end_date: [str or date] the end date of the predictions
    :param spatial_idx_name: [str] name of column that is used for spatial
        index (e.g., 'seg_id_nat')
    :param time_idx_name: [str] name of column that is used for temporal index
        (usually 'time')
    :param log_vars: [list-like] which variables_to_log (if any) were logged in data
    prep
    :param keep_last_frac: [float] fraction of the predictions to keep starting
    from the *end* of the predictions (0-1). (1 means you keep all of the
    predictions, .75 means you keep the final three quarters of the predictions).
    Values greater than 1 are taken as a constant number of predictions to keep from the
    end of the sequence.
    :param offset: [float] 0-1, how to offset the batches (e.g., 0.5 means that
    the first batch will be 0-365 and the second will be 182-547). Values greater than
    1 are taken as a constant number of observations to offset by.
    :param swap_halves_of_first_seq: [bool] whether or not to make an
    *additional* sequence from the first sequence. The additional sequence will
    be the first sequence with the first and last halves swapped. The last half
    of the the first sequence serves as a stand-in spin-up period for ths first
    half predictions. This option makes most sense only when keep_last_portion=0.5.
    :return: [pd dataframe] the predictions
    """
    ds_x_scaled = ds_x_scaled[train_io_data["x_vars"]]
    x_data = ds_x_scaled.sel(date=slice(start_date, end_date))
    x_batches = convert_batch_reshape(
        x_data,
        seq_len=seq_len,
        offset=offset,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
    )
    x_batch_ids = coord_as_reshaped_array(
        x_data,
        spatial_idx_name,
        seq_len=seq_len,
        offset=offset,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
    )
    x_batch_dates = coord_as_reshaped_array(
        x_data,
        time_idx_name,
        seq_len=seq_len,
        offset=offset,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
    )
    num_segs = len(np.unique(x_batch_ids))

    if swap_halves_of_first_seq:
        x_batches = swap_first_seq_halves(x_batches, num_segs)
        x_batch_ids = swap_first_seq_halves(x_batch_ids, num_segs)
        x_batch_dates = swap_first_seq_halves(x_batch_dates, num_segs)

    predictions = predict(
        model,
        x_batches,
        x_batch_ids,
        x_batch_dates,
        train_io_data["y_std"],
        train_io_data["y_mean"],
        train_io_data["y_obs_vars"],
        keep_last_portion=keep_last_frac,
        log_vars=log_vars,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name
    )
    return predictions


def predict_from_arbitrary_data(
    raw_data_file,
    pred_start_date,
    pred_end_date,
    train_io_data,
    model,
    spatial_idx_name="site_id",
    time_idx_name="time",
    seq_len=365,
    log_vars=None,
):
    """
    make predictions given raw data that is potentially independent from the
    data used to train the model

    :param raw_data_file: [str] path to zarr dataset with x data that you want
    to use to make predictions
    :param pred_start_date: [str] start date of predictions (fmt: YYYY-MM-DD)
    :param pred_end_date: [str] end date of predictions (fmt: YYYY-MM-DD)
    :param train_io_data: [str or np NpzFile] the path to or the loaded data
    that was used to train the model. This file must contain the variables_to_log
    names, the standard deviations, and the means of the X and Y variables_to_log. Only
    in with this information can the model be used properly
    :param model: [tf model] model to use for predictions
    :param spatial_idx_name: [str] name of column that is used for spatial
        index (e.g., 'seg_id_nat')
    :param time_idx_name: [str] name of column that is used for temporal index
        (usually 'time')
    :param seq_len: [int] length of input sequences given to model
    :param flow_in_temp: [bool] whether the flow should be an input into temp
    for the rgcn model
    :param log_vars: [list-like] which variables_to_log (if any) were logged in data
    prep
    :return: [pd dataframe] the predictions
    """
    train_io_data = get_data_if_file(train_io_data)

    ds = xr.open_zarr(raw_data_file)

    ds_x = ds[train_io_data["x_vars"]]

    x_stds = mean_or_std_dataset_from_np(train_io_data, "x_std", "x_vars")
    x_means = mean_or_std_dataset_from_np(train_io_data, "x_mean", "x_vars")

    ds_x_scaled, _, _ = scale(ds_x, std=x_stds, mean=x_means)

    pred_start_date = datetime.datetime.strptime(pred_start_date, "%Y-%m-%d")
    # look back half of the sequence length before the prediction start date.
    # if present, this serves as a half-sequence warm-up period
    inputs_start_date = pred_start_date - datetime.timedelta(round(seq_len / 2))

    # get the "middle" predictions
    middle_predictions = predict_one_date_range(
        model,
        ds_x_scaled,
        train_io_data,
        seq_len,
        inputs_start_date,
        pred_end_date,
        log_vars=log_vars,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
        keep_last_frac=0.5,
        offset=0.5,
    )

    # get the "beginning" predictions
    start_dates_end = pred_start_date + datetime.timedelta(seq_len)

    beginning_predictions = predict_one_date_range(
        model,
        ds_x_scaled,
        train_io_data,
        seq_len,
        pred_start_date,
        start_dates_end,
        log_vars=log_vars,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
        keep_last_frac=1,
        offset=0.5,
        swap_halves_of_first_seq=True,
    )

    # get the "end" predictions
    end_date_end = datetime.datetime.strptime(
        pred_end_date, "%Y-%m-%d"
    ) + datetime.timedelta(1)
    end_dates_start = end_date_end - datetime.timedelta(seq_len)

    end_predictions = predict_one_date_range(
        model,
        ds_x_scaled,
        train_io_data,
        seq_len,
        end_dates_start,
        end_date_end,
        log_vars=log_vars,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
        keep_last_frac=1,
        offset=1,
    )

    # trim beginning and end predictions
    predictions_beginning_trim = beginning_predictions[
        beginning_predictions[time_idx_name] < middle_predictions[time_idx_name].min()
    ]
    predictions_end_trim = end_predictions[
        end_predictions[time_idx_name] > middle_predictions[time_idx_name].max()
    ]

    predictions_combined = pd.concat(
        [predictions_beginning_trim, middle_predictions, predictions_end_trim]
    )
    return predictions_combined


def predict_encoder_decoder(
    model,
    head,
    x_encoder,
    x_decoder,
    site_ids,
    init_dates,
    target_vars,
    outfile=None,
    spatial_idx_name="site_id",
    time_idx_name="time",
    decoder_seq_len=10,
    target_log_transformed=False,
):
    """
    Make predictions from trained encoder-decoder model.

    Parameters:
        model: Trained EncoderDecoderLSTM model
        head: Head type ("CMAL" or "GMM")
        x_encoder: Encoder input data [n_samples, encoder_seq_len, n_encoder_feat]
        x_decoder: Decoder input data [n_samples, decoder_seq_len, n_decoder_feat]
        site_ids: Site IDs for each sample [n_samples]
        init_dates: Initialization dates for each sample [n_samples]
        target_vars: Names of target variables
        outfile: Path to save predictions as feather file
        spatial_idx_name: Name for site ID column
        time_idx_name: Name for time column
        decoder_seq_len: Length of decoder sequence (forecast horizon)
        target_log_transformed: If True, all distribution parameters (mu, b, tau)
            remain in log-space. Quantiles should be computed in log-space first,
            then back-transformed using exp(q) - 0.01.

    Returns:
        pd.DataFrame with predictions including site_id, time, lead_time, and
        distribution parameters (mu, b, tau for CMAL or mu, sigma for GMM).
        If target_log_transformed=True, a 'log_transformed' column is added to
        indicate all parameters are in log-space (use qALD then exp() - 0.01).
    """
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)
    model.eval()

    # Convert to tensors
    x_encoder_t = torch.from_numpy(x_encoder).float().to(device)
    x_decoder_t = torch.from_numpy(x_decoder).float().to(device)

    # Run prediction
    with torch.no_grad():
        output, (h_final, c_final) = model(x_encoder_t, x_decoder_t)

    # Extract distribution parameters
    if head == "CMAL":
        mu = output['mu'].cpu().numpy()  # [n_samples, decoder_seq_len, n_targets]
        b = output['b'].cpu().numpy()
        tau = output['tau'].cpu().numpy()

        # NOTE: When target_log_transformed=True, all parameters (mu, b, tau) stay in log-space
        # The back-transformation happens in R when computing quantiles:
        #   1. Compute quantiles in log-space using qALD(mu, b, tau)
        #   2. Back-transform quantiles: exp(q) - 0.01
        # This ensures proper uncertainty propagation through the nonlinear transform

    elif head == "GMM":
        mu = output['mu'].cpu().numpy()
        sigma = output['sigma'].cpu().numpy()

        # NOTE: When target_log_transformed=True, mu and sigma stay in log-space
        # Back-transformation happens when computing quantiles
    else:
        raise ValueError(f"Head type {head} not supported for encoder-decoder predictions")

    # Determine family string
    if head == "CMAL":
        family = 'log_asymmetric_laplace' if target_log_transformed else 'asymmetric_laplace'
    elif head == "GMM":
        family = 'normal'

    # Build EFI long-format dataframe with predictions for each lead time
    records = []
    n_samples = x_encoder.shape[0]

    for i in range(n_samples):
        # Convert to date string to avoid timezone issues when reading in R
        # numpy datetime64 -> pandas Timestamp -> date string -> back to Timestamp at midnight
        raw_date = pd.to_datetime(init_dates[i])
        date_str = str(raw_date.date())  # 'YYYY-MM-DD' format
        reference_datetime = pd.Timestamp(date_str)  # Creates timestamp at midnight, no timezone
        site_id = site_ids[i]

        for lead in range(decoder_seq_len):
            forecast_date = reference_datetime + pd.Timedelta(days=lead)

            for var_idx, var_name in enumerate(target_vars):
                if head == "CMAL":
                    records.append({
                        'reference_datetime': reference_datetime,
                        'datetime': forecast_date,
                        'duration': 'P1D',
                        spatial_idx_name: site_id,
                        'family': family,
                        'parameter': 'location',
                        'variable': var_name,
                        'prediction': mu[i, lead, var_idx],
                    })
                    records.append({
                        'reference_datetime': reference_datetime,
                        'datetime': forecast_date,
                        'duration': 'P1D',
                        spatial_idx_name: site_id,
                        'family': family,
                        'parameter': 'scale',
                        'variable': var_name,
                        'prediction': b[i, lead, var_idx],
                    })
                    records.append({
                        'reference_datetime': reference_datetime,
                        'datetime': forecast_date,
                        'duration': 'P1D',
                        spatial_idx_name: site_id,
                        'family': family,
                        'parameter': 'asymmetry',
                        'variable': var_name,
                        'prediction': tau[i, lead, var_idx],
                    })
                elif head == "GMM":
                    records.append({
                        'reference_datetime': reference_datetime,
                        'datetime': forecast_date,
                        'duration': 'P1D',
                        spatial_idx_name: site_id,
                        'family': family,
                        'parameter': 'mu',
                        'variable': var_name,
                        'prediction': mu[i, lead, var_idx],
                    })
                    records.append({
                        'reference_datetime': reference_datetime,
                        'datetime': forecast_date,
                        'duration': 'P1D',
                        spatial_idx_name: site_id,
                        'family': family,
                        'parameter': 'sigma',
                        'variable': var_name,
                        'prediction': sigma[i, lead, var_idx],
                    })

    df = pd.DataFrame(records)

    if outfile:
        df.to_feather(outfile)
        print(f"Saved encoder-decoder predictions to {outfile}")

    return df


def predict_encoder_decoder_from_io_data(
    model,
    head,
    io_data,
    partition,
    outfile,
    spatial_idx_name="site_id",
    time_idx_name="time",
):
    """
    Make predictions from encoder-decoder model using data from .npz file.

    Parameters:
        model: Trained EncoderDecoderLSTM model
        head: Head type ("CMAL" or "GMM")
        io_data: Path to .npz file or loaded dict with encoder-decoder data
        partition: Data partition ("train" or "val")
        outfile: Path to save predictions as feather file
        spatial_idx_name: Name for site ID column
        time_idx_name: Name for time column

    Returns:
        pd.DataFrame with predictions
    """
    io_data = get_data_if_file(io_data)

    # Get data for the specified partition
    x_encoder = io_data[f'x_encoder_{partition}']
    x_decoder = io_data[f'x_decoder_{partition}']
    site_ids = io_data[f'site_ids_{partition}']
    init_dates = io_data[f'init_dates_{partition}']
    target_vars = io_data['target_vars']
    decoder_seq_len = io_data.get('decoder_seq_len', x_decoder.shape[1])
    target_log_transformed = bool(io_data.get('target_log_transformed', False))

    return predict_encoder_decoder(
        model=model,
        head=head,
        x_encoder=x_encoder,
        x_decoder=x_decoder,
        site_ids=site_ids,
        init_dates=init_dates,
        target_vars=target_vars,
        outfile=outfile,
        spatial_idx_name=spatial_idx_name,
        time_idx_name=time_idx_name,
        decoder_seq_len=decoder_seq_len,
        target_log_transformed=target_log_transformed,
    )


def predict_torch(x_data, model, head, batch_size, h, c, weighting_matrix):
    """
    from river-dl
    @param model: [object] initialized torch model
    @param batch_size: [int]
    @param device: [str] cuda or cpu
    @return: [tensor] predicted values
    """
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    
    model.to(device)
    data = []
    for i in range(len(x_data)):
        data.append(torch.from_numpy(x_data[i]).float())
    
    weighting_matrix = torch.from_numpy(weighting_matrix).float() 
    h = torch.from_numpy(h).float()
    c = torch.from_numpy(c).float()

    dataloader = torch.utils.data.DataLoader(data, batch_size=batch_size, shuffle=False, pin_memory=True)
    model.eval()
    if head == "GMM":
        predicted_mu = []
        predicted_sigma = []
    if head == "CMAL":
        predicted_mu = []
        predicted_b = []
        predicted_tau = [] 
    # TODO: update this if multiple mixtures 
    for iter, x in enumerate(dataloader):
        trainx = x.to(device)
        with torch.no_grad():
            output, (h, c) = model(trainx, (h, c), weighting_matrix)
        if head == "GMM": 
            predicted_mu.append(output['mu'].detach())
            predicted_sigma.append(output['sigma'].detach())
        if head == "CMAL":
            predicted_mu.append(output['mu'].detach())
            predicted_b.append(output['b'].detach())
            predicted_tau.append(output['tau'].detach())
    if head == "GMM":
        predicted_mu = torch.cat(predicted_mu, dim=0)
        predicted_sigma = torch.cat(predicted_sigma, dim=0)
        return predicted_mu, predicted_sigma
    if head == "CMAL":
        predicted_mu = torch.cat(predicted_mu, dim=0)
        predicted_b = torch.cat(predicted_b, dim=0)
        predicted_tau = torch.cat(predicted_tau, dim=0)
        return predicted_mu, predicted_b, predicted_tau

def prepped_array_to_df(data_array, head, dates, ids, col_names, spatial_idx_name='site_id', time_idx_name='time', log_transform=False):
    """
    Convert prepped output data in numpy arrays to EFI long-format pandas DataFrame.

    :param data_array: tuple of numpy arrays (mu, sigma) for GMM or (mu, b, tau) for CMAL
    :param head: [str] "GMM" or "CMAL"
    :param dates: [numpy array] array of dates [nbatch, seq_len, ...]
    :param ids: [numpy array] array of site ids [nbatch, seq_len, ...]
    :param col_names: [list] target variable names (e.g., ['chla'])
    :param spatial_idx_name: [str] name of site ID column (e.g., 'site_id')
    :param time_idx_name: [str] unused (kept for API compatibility)
    :param log_transform: [bool] whether predictions are in log-space
    :return: [pd.DataFrame] EFI long format with columns:
        reference_datetime, datetime, duration, site_id, family, parameter, variable, prediction
    """
    num_out_vars = data_array[0].shape[-1]
    dates_flat = dates.flatten()
    ids_flat = ids.flatten()

    if head == "GMM":
        data_array_mu = data_array[0]
        data_array_sigma = data_array[1]
        family = 'normal'
        parts = []
        for i, var_name in enumerate(col_names):
            mu_flat = data_array_mu[..., i].flatten()
            sigma_flat = data_array_sigma[..., i].flatten()
            parts.append(pd.DataFrame({
                'datetime': dates_flat,
                spatial_idx_name: ids_flat,
                'variable': var_name,
                'parameter': 'mu',
                'prediction': mu_flat,
            }))
            parts.append(pd.DataFrame({
                'datetime': dates_flat,
                spatial_idx_name: ids_flat,
                'variable': var_name,
                'parameter': 'sigma',
                'prediction': sigma_flat,
            }))
        df = pd.concat(parts, ignore_index=True)
        df['reference_datetime'] = df['datetime']
        df['duration'] = 'P1D'
        df['family'] = family
        df = df[['reference_datetime', 'datetime', 'duration', spatial_idx_name, 'family', 'parameter', 'variable', 'prediction']]

    elif head == "CMAL":
        data_array_mu = data_array[0]
        data_array_b = data_array[1]
        data_array_tau = data_array[2]
        family = 'log_asymmetric_laplace' if log_transform else 'asymmetric_laplace'
        parts = []
        for i, var_name in enumerate(col_names):
            mu_flat = data_array_mu[..., i].flatten()
            b_flat = data_array_b[..., i].flatten()
            tau_flat = data_array_tau[..., i].flatten()
            parts.append(pd.DataFrame({
                'datetime': dates_flat,
                spatial_idx_name: ids_flat,
                'variable': var_name,
                'parameter': 'location',
                'prediction': mu_flat,
            }))
            parts.append(pd.DataFrame({
                'datetime': dates_flat,
                spatial_idx_name: ids_flat,
                'variable': var_name,
                'parameter': 'scale',
                'prediction': b_flat,
            }))
            parts.append(pd.DataFrame({
                'datetime': dates_flat,
                spatial_idx_name: ids_flat,
                'variable': var_name,
                'parameter': 'asymmetry',
                'prediction': tau_flat,
            }))
        df = pd.concat(parts, ignore_index=True)
        df['reference_datetime'] = df['datetime']
        df['duration'] = 'P1D'
        df['family'] = family
        df = df[['reference_datetime', 'datetime', 'duration', spatial_idx_name, 'family', 'parameter', 'variable', 'prediction']]
    else:
        raise ValueError(f"Head type {head} not supported")

    return df


def fmt_preds_obs(pred_data,
                  obs_data,
                  spatial_idx_name="site_id",
                  time_idx_name="time"):
    """
    combine predictions and observations in one dataframe
    :param pred_data:[str] filepath to the predictions file
    :param obs_file:[str] filepath to the observations file
    :param spatial_idx_name: [str] name of column that is used for spatial
        index (e.g., 'seg_id_nat')
    :param time_idx_name: [str] name of column that is used for temporal index
        (usually 'time')
    """
    pred_data = load_if_not_df(pred_data)

    if {time_idx_name, spatial_idx_name}.issubset(pred_data.columns):
        pred_data.set_index([time_idx_name, spatial_idx_name], inplace=True)
        
    obs = load_if_not_df(obs_data)
    
    if {time_idx_name, spatial_idx_name}.issubset(obs.columns):
        obs.set_index([time_idx_name, spatial_idx_name], inplace=True)

    variables_data = {}

    for var_name in pred_data.columns:
        obs_var = obs.copy()
        obs_var = obs_var[[var_name]]
        obs_var.columns = ["obs"]
        preds_var = pred_data[[var_name]]
        preds_var.columns = ["pred"]
        # trimming obs to preds speeds up following join greatly
        obs_var = trim_obs(obs_var, preds_var, spatial_idx_name, time_idx_name)
        combined = preds_var.join(obs_var)
        variables_data[var_name] = combined
    return variables_data


def trim_obs(obs, preds, spatial_idx_name="site_id", time_idx_name="time"):
    obs_trim = obs.reset_index()
    trim_preds = preds.reset_index()
    obs_trim = obs_trim[
        (obs_trim[time_idx_name] >= trim_preds[time_idx_name].min())
        & (obs_trim[time_idx_name] <= trim_preds[time_idx_name].max())
        & (obs_trim[spatial_idx_name].isin(trim_preds[spatial_idx_name].unique()))
    ]
    return obs_trim.set_index([time_idx_name, spatial_idx_name])


def load_if_not_df(pred_data):
    if isinstance(pred_data, str):
        return pd.read_feather(pred_data)
    else:
        return pred_data
