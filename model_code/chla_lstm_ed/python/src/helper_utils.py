import os

import yaml
from datetime import datetime

# CSDMS Standard Names for ALD chlorophyll output parameters
CSDMS_CHLA_LOC   = 'channel_water_surface_water__location_of_ald_of_chlorophyll'
CSDMS_CHLA_SCALE = 'channel_water_surface_water__scale_of_ald_of_chlorophyll'
CSDMS_CHLA_ASYM  = 'channel_water_surface_water__asymmetry_of_ald_of_chlorophyll'

# Decoder feature: last observed chla available at forecast time (the final encoder
# day's chla_lagged), repeated over every forecast day (config decoder_last_chla).
LAST_CHLA_VAR = 'chla_last_obs'


def generate_model_id(config, date_str=None):
    """
    Generate a model ID from config settings.

    Format: YYYYMMDD_modeltype_AR/noAR_sites_head_h{hidden}[_e{enc}d{dec}]

    Examples:
        - 20260107_lstm_AR_usgs-stack_CMAL_h16
        - 20260107_ed_AR_usgs-stack_CMAL_h16_e365d10

    Args:
        config (dict): Configuration dictionary
        date_str (str, datetime, pd.Timestamp, optional): Date in YYYYMMDD format string,
                                  or datetime/Timestamp object. If None, uses today's date.

    Returns:
        str: Generated model ID
    """
    import pandas as pd

    # Date component - handle various input types
    if date_str is None or pd.isnull(date_str):
        # None or NaT - use today's date
        date_str = datetime.now().strftime("%Y%m%d")
    elif hasattr(date_str, 'strftime'):
        # datetime or pd.Timestamp object
        date_str = date_str.strftime("%Y%m%d")
    elif isinstance(date_str, str):
        # Already a string - remove any hyphens if present
        date_str = date_str.replace("-", "")

    # Model type: 'ed' for encoder-decoder, 'lstm' for standard
    model_type_raw = config.get("model_type", "lstm")
    model_type = "ed" if model_type_raw == "encoder_decoder" else "lstm"

    # AR status based on lag_target setting
    ar_status = "AR" if config.get("lag_target", False) else "noAR"

    # Sites (abbreviated for readability)
    site_abbrevs = {
        "usgsrc4cast": "usgs",
        "stackpoole": "stack",
        "savoy": "sav",
        "vera4cast": "vera"
    }
    sites_list = config.get("sites_to_include", [])
    sites = "-".join(site_abbrevs.get(s, s[:4]) for s in sites_list)

    # Head type (CMAL, GMM, etc.)
    head = config.get("head", "reg")

    # Hidden units
    hidden = f"h{config.get('hidden_units', 16)}"

    # Build the ID
    parts = [date_str, model_type, ar_status, sites, head, hidden]

    # Add encoder-decoder specific params if applicable
    if model_type == "ed":
        enc_len = config.get("encoder_seq_len", 365)
        dec_len = config.get("decoder_seq_len", 10)
        parts.append(f"e{enc_len}d{dec_len}")

    return "_".join(parts)


def get_model_id(config):
    """
    Get the model ID from config, auto-generating if not explicitly set.

    If config contains 'model_id', uses that value.
    If config contains 'model_date', generates ID using that date.
    Otherwise, generates ID using today's date.

    Args:
        config (dict): Configuration dictionary

    Returns:
        str: Model ID (either from config or auto-generated)
    """
    # If explicit model_id is provided and not empty, use it
    if config.get("model_id"):
        return config["model_id"]

    # Otherwise, generate from config
    date_str = config.get("model_date")  # Optional date override
    return generate_model_id(config, date_str)


def chla_lag_source(config):
    """Resolve the lagged-chla data source from config flags.

    Returns None when no lag is requested, otherwise:
      - 'realtime': observed VERA4cast ``Chla_ugL_mean`` (``chla_lag: True``)
      - 'noAR': gap-filled noAR-model predictions (``lag_target: True``)

    ``chla_lag`` takes precedence so the two lag mechanisms don't stack.
    """
    if config.get("chla_lag"):
        return "realtime"
    if config.get("lag_target"):
        return "noAR"
    return None


def chla_lag_days(config, default=1):
    """Lag offset for the real-time ``chla_lag`` source.

    Kept separate from ``lag_days`` (which drives the ``lag_target``/noAR path and
    is conventionally 0 for the encoder-decoder) so the lag is explicit.
    """
    return int(config.get("chla_lag_days", default))


def met_driver_lag_days(config, default=1):
    """Lag, in days, between the forecast reference date and the met driver init.

    The current day's met drivers are not available in real time (GEFS stage2
    publishes ``reference_datetime`` one or more days behind real time), so a
    forecast initialized on date R must be driven by the most recent available
    driver -- ``R - met_driver_lag_days`` by default. Set ``met_driver_lag_days:
    0`` only for hindcasts, where the same-day analysis/forecast does exist.
    """
    return int(config.get("met_driver_lag_days", default))


def load_config(yaml_file: str):
    """
    Loads the configuration from a YAML file.

    Args:
        yaml_file (str): The path to the YAML file.

    Returns:
        dict: The loaded configuration.
    """
    with open(yaml_file, 'r') as stream:
        config = yaml.safe_load(stream)
    return config



def training_data_dir(config):
    """Directory holding the training ``{model_id}.npz`` files.

    ``config['training_data_dir']`` when set, otherwise ``<data_in_dir>/training_data``.
    """
    return config.get("training_data_dir") or os.path.join(config.get("data_in_dir", "in/"), "training_data")


def check_no_overwrite(paths, config):
    """Refuse to overwrite existing training outputs.

    Raises FileExistsError if any of ``paths`` already exists, unless
    ``config['overwrite_training_outputs']`` is True. Protects previously trained
    models (weights, scalers, logs) from being replaced by a new run that resolves
    to the same model_id or output directory.
    """
    if config.get("overwrite_training_outputs", False):
        return
    existing = [str(p) for p in paths if os.path.exists(p)]
    if existing:
        raise FileExistsError(
            "Refusing to overwrite existing training output(s): "
            + ", ".join(existing)
            + ". Change model_date/model_id or train_dir/training_data_dir, or set "
            "overwrite_training_outputs: True to replace them."
        )
