import os
import yaml
# Configuration file functionality
from pathlib import Path
# Basic utilities
import numpy as np
import pandas as pd
# Need these for BMI
from bmipy import Bmi
# LSTM here is based on PyTorch
import torch
from scipy.stats import laplace_asymmetric

from src.torch_models import *
from src.training_utils import *
from src.training_utils import train_torch_encoder_decoder
from src.sampling_utils import *
from src.predict_utils import *
from src.helper_utils import (get_model_id, training_data_dir, check_no_overwrite, LAST_CHLA_VAR,
                              CSDMS_CHLA_LOC, CSDMS_CHLA_SCALE, CSDMS_CHLA_ASYM)

USE_PATH = True

class bmi_lstm(Bmi):

    def __init__(self):
        """
        Initializes the BMI LSTM model for stream chlorophyll prediction with necessary attributes and sets up the model for simulation.

        Attributes:
        - _name (str): Name of the model, set to "LSTM for Stream Chlorophyll".
        - _values (dict): Dictionary to store model variables.
        - _var_loc (str): Location where model variables are defined, set to "node".
        - _start_time (float): Start time of the simulation, set to 0.
        - _end_time (float): End time of the simulation, set to maximum possible float value.
        - _time_units (str): Units of time used in the simulation, set to "day".
        - _time_step_size (float): Size of the time step used in the simulation, set to 1.0.
        - _input_var_names (list): A list of input variable names (CSDMS standard names).
        - _output_var_names (list): A list of output variable names (CSDMS standard names).
        - _var_name_units_map (dict): A dictionary mapping CSDMS standard names to internal variable names and units.

        Note: The _end_time attribute is set to the maximum possible float value to indicate that there is no predefined end time for the simulation.

        Static Attributes:
        - _att_map (dict): Dictionary containing static attributes of the model such as model_name, version, and author_name.

        Author:
        - Jacob Zwart (with model contributions from Jeremy Diaz and others)
        """
        super(bmi_lstm, self).__init__()
        self._name = "LSTM for Stream Chlorophyll"
        self._values = {}
        self._var_loc = "node"
        # self._var_grid_id = 0
        # self._var_grid_type = "scalar"
        self._start_time = 0
        self._end_time = np.finfo("d").max
        self._time_units = "day"
        self._time_step_size = 1.0

    #----------------------------------------------
    # Static attributes of the model
    #----------------------------------------------
    _att_map = {
        'model_name':         'LSTM for Stream Chlorophyll',
        'version':            '1.0',
        'author_name':        'Jacob Zwart' } # Jeremy Diaz and many others

    #---------------------------------------------
    # Input variable names (CSDMS standard names)
    #---------------------------------------------
    _input_var_names = ['land_surface_air__min_of_temperature',
                        'land_surface_air__max_of_temperature',
                        'land_surface_air__mean_of_temperature',
                        'land_surface_radiation~incoming~shortwave__energy_flux',
                        'land_surface_radiation~incoming~longwave__energy_flux',
                        'atmosphere_water__liquid_equivalent_precipitation_rate',
                        'atmosphere_clouds__coverage',
                        'land_surface_wind__x_component_of_velocity',
                        'land_surface_wind__y_component_of_velocity',
                        'channel_water_x-section__volume_flow_rate',
                        'channel_exit_center__latitude',
                        'channel_exit_center__longitude',
                        'channel_water_surface_water__median_of_chlorophyll~yesterday',
                        'channel_water_surface_water__source_of_chlorophyll~yesterday']

    #---------------------------------------------
    # Output variable names (CSDMS standard names)
    #---------------------------------------------
    _output_var_names = ['channel_water_surface_water__location_of_ald_of_chlorophyll',
                        'channel_water_surface_water__scale_of_ald_of_chlorophyll',
                        'channel_water_surface_water__asymmetry_of_ald_of_chlorophyll']

    #------------------------------------------------------
    # Create a Python dictionary that maps CSDMS Standard
    # Names to the model's internal variable names.
    #------------------------------------------------------
    _var_name_units_map = {
                                'land_surface_air__min_of_temperature':['minimum_temperature_2m', 'degC'],
                                'land_surface_air__max_of_temperature':['maximum_temperature_2m', 'degC'],
                                'land_surface_air__mean_of_temperature':['temperature_2m', 'degC'],
                                'land_surface_radiation~incoming~shortwave__energy_flux':['downward_short_wave_radiation_flux_surface', 'W m-2'],
                                'land_surface_radiation~incoming~longwave__energy_flux':['downward_long_wave_radiation_flux_surface', 'W m-2'],
                                'atmosphere_water__liquid_equivalent_precipitation_rate':['precipitation_surface', 'mm s-1'],
                                'atmosphere_clouds__coverage': ['total_cloud_cover_atmosphere', 'percent'],
                                'land_surface_wind__x_component_of_velocity':['wind_u_10m', 'm s-1'],
                                'land_surface_wind__y_component_of_velocity':['wind_v_10m', 'm s-1'],
                                'channel_water_x-section__volume_flow_rate':['river_discharge', 'cms'],
                                'channel_water_surface_water__median_of_chlorophyll~yesterday':['chla_lagged', 'ug L-1'],
                                'channel_water_surface_water__source_of_chlorophyll~yesterday':['chla_source_lagged', 'binary'],
                                'channel_exit_center__latitude':['latitude', 'decimal degrees'],
                                'channel_exit_center__longitude':['longitude', 'decimal degrees'],
                                'channel_water_surface_water__location_of_ald_of_chlorophyll':['chla_location', 'ug L-1'],
                                'channel_water_surface_water__scale_of_ald_of_chlorophyll':['chla_scale', 'ug L-1'],
                                'channel_water_surface_water__asymmetry_of_ald_of_chlorophyll':['chla_asymmetry', 'ug L-1']
                        }

    def __getattribute__(self, item):
        """
        Customize instance attribute access.

        For those items that correspond to BMI input or output variables (which should be in numpy arrays) and have
        values that are just a single-element array, deviate from the standard behavior and return the single array
        element. Fall back to the default behavior in any other case.

        This supports having a BMI variable be backed by a numpy array, while also allowing the attribute to be used as
        just a scalar, as it is in many places for this type.

        Parameters:
        - item (str): The name of the attribute item to get.

        Returns:
        The value of the named item.
        """
        # Have these work explicitly (or else loops)
        if item == '_input_var_names' or item == '_output_var_names':
            return super(bmi_lstm, self).__getattribute__(item)

        # By default, for things other than BMI variables, use normal behavior
        if item not in super(bmi_lstm, self).__getattribute__('_input_var_names') and item not in super(bmi_lstm, self).__getattribute__('_output_var_names'):
            return super(bmi_lstm, self).__getattribute__(item)

        # Return the single scalar value from any ndarray of size 1
        value = super(bmi_lstm, self).__getattribute__(item)
        if isinstance(value, np.ndarray) and value.size == 1:
            return value[0]
        else:
            return value


    def __setattr__(self, key, value):
        """
        Customized instance attribute mutator functionality.

        For those attribute with keys indicating they are a BMI input or output variable (which should be in numpy
        arrays), wrap any scalar ``value`` as a one-element numpy array and use that in a nested call to the superclass
        implementation of this function.  In any other cases, just pass the given ``key`` and ``value`` to a nested
        call.

        This supports automatically having a BMI variable be backed by a numpy array, even if it is initialized using a
        scalar, while otherwise maintaining standard behavior.

        Parameters:
        - key (str): The name of the attribute to set.
        - value: The value to assign to the attribute.

        Returns:
        None
        """
        # Have these work explicitly (or else loops)
        if key == '_input_var_names' or key == '_output_var_names':
            super(bmi_lstm, self).__setattr__(key, value)

        # Pass thru if value is already an array
        if isinstance(value, np.ndarray):
            super(bmi_lstm, self).__setattr__(key, value)
        # Override to put scalars into ndarray for BMI input/output variables
        elif key in self._input_var_names or key in self._output_var_names:
            super(bmi_lstm, self).__setattr__(key, np.array([value]))
        # By default, use normal behavior
        else:
            super(bmi_lstm, self).__setattr__(key, value)


    #------------------------------------------------------------
    #------------------------------------------------------------
    # BMI: Model Control Functions
    #------------------------------------------------------------
    #------------------------------------------------------------

    #-------------------------------------------------------------------
    def initialize(self, config_file = None, torch_seed = None, train = False, root_dir = None):
        """Initialize the BMI LSTM model with BMI configuration file.

        This function initializes the BMI LSTM model by performing the following steps:
        - Creates lookup tables for variable names and units.
        - Initializes all variables to zero.
        - Reads the BMI configuration file.
        - Loads training configurations and data.
        - Loads scaler values and LSTM states from trained model.
        - Initializes an LSTM model with specified parameters.
        - Initializes values for input to the LSTM.
        - Loads pre-trained LSTM weights.
        - Sets the simulation start time.
        - Retrieves verbosity level from the BMI configuration.

        Parameters:
            config_file (str): Path to the BMI configuration file in YAML format. Default is None.

        Note:
            The BMI configuration file directs the subsequent actions of the model.
        """

        # ----- Create some lookup tables from the long variable names --------#
        self._var_name_map_long_first = {long_name:self._var_name_units_map[long_name][0] for \
                                         long_name in self._var_name_units_map.keys()}
        self._var_name_map_short_first = {self._var_name_units_map[long_name][0]:long_name for \
                                          long_name in self._var_name_units_map.keys()}
        self._var_units_map = {long_name:self._var_name_units_map[long_name][1] for \
                                          long_name in self._var_name_units_map.keys()}

        # -------------- Initalize all the variables --------------------------#
        # -------------- so that they'll be picked up with the get functions --#
        for var_name in list(self._var_name_units_map.keys()):
            # ---------- All the variables are single values ------------------#
            # ---------- so just set to zero for now.        ------------------#
            self._values[var_name] = 0
            setattr(self, var_name, 0)

        # -------------- Read in the BMI configuration -------------------------#
        # This will direct all the next moves.
        if config_file is not None:
            config_file = Path(config_file)
            #----------------------------------------------------------
            # Note: config_file should have type 'str', vs. being a
            #       Path object. So apply Path in initialize().
            #----------------------------------------------------------
            if not os.path.exists(config_file):
                raise FileNotFoundError(f"Configuration file {config_file} not found.")

            with open(config_file, 'r') as fp:
                cfg = yaml.safe_load(fp)
            self.cfg_bmi = self._parse_config(cfg)
        else:
            raise ValueError("Error: No configuration provided, nothing to do...")
        # ---------- Set torch random seed -----------------
        seed = None
        if torch_seed is not None:
            seed = torch_seed
        elif config_file is not None:
            seed = self.cfg_bmi.get("seed")
        if seed is not None:
            print(f"Set torch seed to {seed}")
            torch.manual_seed(seed)
        else:
            print("No torch seed is assigned")

        if root_dir is not None:
            self.cfg_bmi['root_dir'] = root_dir
        # ------------- Load in the configuration file for the specific LSTM --#
        # This will include all the details about how the model was trained
        self.get_training_configurations()
        self.get_data(train=train)

        if train:
            print("Initialized model for training. Use <model_object_name>.train_model() to start the training")

        else:
            # scalar values
            self.get_scaler_values()

            # ------------- Initialize model based on architecture type --------------#
            if self.model_type == 'encoder_decoder':
                self._initialize_encoder_decoder_model()
                # Anchor the simulation time at the forecast reference date so
                # get_current_date() returns it during inference.
                self._set_reference_time()
            else:
                self._initialize_standard_lstm_model()
                # start of the simulation time
                self.t = self._start_time

        # Gather verbosity lvl from bmi-config for stdout printing, etc.
        self.verbose = self.cfg_bmi['verbose']

    def _initialize_standard_lstm_model(self):
        """Initialize the standard LSTM model (non-encoder-decoder architecture).

        This helper method initializes the LSTMWithHead model for the standard
        autoregressive forecasting approach.
        """
        self.h_t = torch.from_numpy(self.start_h_all_dates).float()
        self.c_t = torch.from_numpy(self.start_c_all_dates).float()
        self.dist_mat_all = torch.from_numpy(self.dist_mat_all).float()

        # Initialize LSTM model
        self.lstm = LSTMWithHead(input_dim=self.n_feat,
                                lstm_hidden_dim=self.hidden_units,
                                adj_matrix=self.dist_mat_all,
                                dropout=self.dropout_rate,
                                recur_dropout=self.recurrent_dropout_rate,
                                head=self.head,
                                head_hidden_dim=self.head_hidden_dim,
                                head_n_dist=self.head_n_dist)

        # Initialize values for the input to the LSTM
        self.initialize_forcings()

        # Load pre-trained weights
        self.lstm.load_state_dict(torch.load(self.weights_dir + '/weights.pth', weights_only=True))

        print(f"Initialized standard LSTM model")
        print(f"  Input features: {self.n_feat}")
        print(f"  Hidden units: {self.hidden_units}")

    def _initialize_encoder_decoder_model(self):
        """Initialize the encoder-decoder LSTM model.

        This helper method initializes the EncoderDecoderLSTM model for the
        encoder-decoder forecasting approach based on Nearing et al. 2024.
        """
        # Get encoder and decoder feature dimensions from loaded data
        # These should be set in filter_data_encoder_decoder or get_scaler_values
        if not hasattr(self, 'n_encoder_feat'):
            # Try to infer from scaling parameters
            self.n_encoder_feat = len(self.encoder_mean) if hasattr(self, 'encoder_mean') else self.n_feat
        if not hasattr(self, 'n_decoder_feat'):
            self.n_decoder_feat = len(self.decoder_mean) if hasattr(self, 'decoder_mean') else self.n_feat

        # Initialize encoder-decoder model
        self.encoder_decoder_model = EncoderDecoderLSTM(
            encoder_input_dim=self.n_encoder_feat,
            decoder_input_dim=self.n_decoder_feat,
            hidden_dim=self.hidden_units,
            head=self.head,
            head_hidden_dim=self.head_hidden_dim,
            head_n_dist=self.head_n_dist,
            dropout=self.dropout_rate,
            recur_dropout=self.recurrent_dropout_rate,
            residual_state_transfer=self.residual_state_transfer
        )

        # Load pre-trained weights
        weights_path = os.path.join(self.weights_dir, 'weights.pth')
        if os.path.exists(weights_path):
            state_dict = torch.load(weights_path, weights_only=True)

            # Backwards compatibility: remap old weight keys to new names
            # Old: hidden_transfer.0.weight/bias -> New: hidden_transfer_linear.weight/bias
            key_mapping = {
                'hidden_transfer.0.weight': 'hidden_transfer_linear.weight',
                'hidden_transfer.0.bias': 'hidden_transfer_linear.bias',
            }
            for old_key, new_key in key_mapping.items():
                if old_key in state_dict and new_key not in state_dict:
                    state_dict[new_key] = state_dict.pop(old_key)
                    print(f"  Remapped weight key: {old_key} -> {new_key}")

            self.encoder_decoder_model.load_state_dict(state_dict)
            print(f"Loaded encoder-decoder weights from {weights_path}")
        else:
            print(f"Warning: No weights found at {weights_path}")

        # Set model to evaluation mode
        self.encoder_decoder_model.eval()

        # Initialize encoder history buffer for rolling window
        # This will store the last encoder_seq_len days of observations
        self.encoder_history = None  # Will be initialized on first forecast

        print(f"Initialized EncoderDecoderLSTM model")
        print(f"  Encoder input dim: {self.n_encoder_feat}")
        print(f"  Decoder input dim: {self.n_decoder_feat}")
        print(f"  Encoder seq len: {self.encoder_seq_len}")
        print(f"  Decoder seq len: {self.decoder_seq_len}")
        print(f"  Hidden units: {self.hidden_units}")
        print(f"  Head: {self.head}")

    def _set_reference_time(self):
        """Set the forecast reference date for encoder-decoder inference.

        The reference date R comes from the file's ``reference_datetime``; it is
        the first forecast day, not part of the encoder window (training encodes
        the ``encoder_seq_len`` days *before* R -- see
        ``create_encoder_decoder_samples``), so it need not be on ``time``.
        Falls back to the last time index when the file carries no
        ``reference_datetime``.
        """
        time_values = self.forecast_data.time.values
        ref = None
        if 'reference_datetime' in self.forecast_data.coords:
            ref = self.forecast_data['reference_datetime'].values
        if ref is None:
            ref = self.forecast_data.attrs.get('reference_datetime')

        self.t = len(time_values) - 1
        if ref is not None:
            self.reference_date = pd.Timestamp(str(ref)).normalize().to_datetime64()
        else:
            self.reference_date = time_values[self.t]
        print(f"Forecast reference date: {self.reference_date}")

    def _update_encoder_decoder(self):
        """Update encoder-decoder model for a single time step.

        This runs the full encoder-decoder model and stores the prediction
        for the first forecast day (day 1). This maintains compatibility with
        the BMI update() interface while using the encoder-decoder architecture.
        """
        # Build encoder and decoder inputs
        x_encoder = self._build_encoder_input()
        x_decoder = self._build_decoder_input(self.decoder_seq_len)

        # Run encoder-decoder model
        self.encoder_decoder_model.eval()
        with torch.no_grad():
            output, (h_final, c_final) = self.encoder_decoder_model(x_encoder, x_decoder)

        # Store the full output for potential use
        self.lstm_output = output

        # Extract first day prediction for BMI output variables
        # Output shape: (n_sites, decoder_seq_len, 1)
        forecasted_mode = output['mu'][:, 0, 0].numpy()
        forecasted_scale = output['b'][:, 0, 0].numpy()
        forecasted_skewness = output['tau'][:, 0, 0].numpy()

        # Set BMI output variables
        setattr(self, 'channel_water_surface_water__location_of_ald_of_chlorophyll', forecasted_mode)
        setattr(self, 'channel_water_surface_water__scale_of_ald_of_chlorophyll', forecasted_scale)
        setattr(self, 'channel_water_surface_water__asymmetry_of_ald_of_chlorophyll', forecasted_skewness)

        # Generate samples if requested
        if self.produce_ensembles:
            if self.head == 'CMAL':
                self.samples = sample_CMAL(output, self.n_samples)
            elif self.head == 'GMM':
                self.samples = sample_GMM(output, self.n_samples)
            self.preds = self.samples

        # Advance time
        self.t += self.get_time_step()


    def update(self):
        """Update the LSTM model for a single time step.

        This function updates the LSTM model for a single time step by performing the following steps:
        - Prepares input data for the LSTM model.
        - Makes predictions using the LSTM model.
        - Generates samples based on the prediction using different uncertainty quantification methods.
        - Saves predicted values to the appropriate output variables.
        - Advances the simulation time by one time step.

        For encoder-decoder models, this runs a full encode-decode pass and returns
        the prediction for the first forecast day.
        """
        # Dispatch to encoder-decoder update if applicable
        if self.model_type == 'encoder_decoder':
            self._update_encoder_decoder()
            return

        self.create_scaled_input_tensor()

        # make predictions
        if self.mc_dropout:
            self.lstm.train()
            self.lstm_output, (self.h_t, self.c_t) = self.lstm(self.input_tensor,
                                                       (self.h_t, self.c_t), self.dist_mat_all)
            self.lstm.eval()
        else:
            self.lstm.eval()
            self.lstm_output, (self.h_t, self.c_t) = self.lstm(self.input_tensor,
                                                       (self.h_t, self.c_t), self.dist_mat_all)
        if self.produce_ensembles:
            if self.head == 'GMM':
                self.samples = sample_GMM(self.lstm_output, self.n_samples)
            if self.head == 'CMAL':
                self.samples = sample_CMAL(self.lstm_output, self.n_samples)
            if self.head == 'UMAL':
                self.samples = sample_UMAL(self.lstm_output, self.n_samples, self.head_n_dist, self.x.shape[0])
            if self.head == 'Regression':
                self.preds = self.lstm_output['y_hat'].detach().numpy()
                self.preds = np.repeat(self.preds, self.n_samples, axis = 2)
            else:
                self.preds = self.samples

        setattr(self, 'channel_water_surface_water__location_of_ald_of_chlorophyll',
                self.lstm_output['mu'].detach().numpy()[:,0,0])
        setattr(self, 'channel_water_surface_water__scale_of_ald_of_chlorophyll',
                self.lstm_output['b'].detach().numpy()[:,0,0])
        setattr(self, 'channel_water_surface_water__asymmetry_of_ald_of_chlorophyll',
                self.lstm_output['tau'].detach().numpy()[:,0,0])

        self.t += self.get_time_step()


    def update_until(self, time):
        """Update model until a particular model time step.
        Parameters
        ----------
        time : float
            Time to run model until.
        """
        cur_step = int(self.get_current_time())

        if time <= cur_step:
            raise ValueError(f"End time, {time}, must be larger than current time, {cur_step}")

        # Check if the requested time to update until extends beyond the available data,
        #  if it does, then throw an error
        input_data_len = len(self.get_value_ptr(self.x_vars[0]))
        if (time+1) != (input_data_len + cur_step):
            target_time = input_data_len + cur_step - 1
            raise ValueError(f"The end time, {time}, does not match the length of data provided.\n Please use an end time of {target_time}, or provide a different amount of input data.")

        self.create_scaled_input_tensor()

        # make predictions
        if self.mc_dropout:
            self.lstm.train()
            self.lstm_output, (self.h_t, self.c_t) = self.lstm(self.input_tensor,
                                                        (self.h_t, self.c_t), self.dist_mat)
            self.lstm.eval()
        else:
            self.lstm.eval()
            self.lstm_output, (self.h_t, self.c_t) = self.lstm(self.input_tensor,
                                                        (self.h_t, self.c_t), self.dist_mat)

        setattr(self, 'channel_water_surface_water__location_of_ald_of_chlorophyll',
                self.lstm_output['mu'].detach().numpy()[:,0,0])
        setattr(self, 'channel_water_surface_water__scale_of_ald_of_chlorophyll',
                self.lstm_output['b'].detach().numpy()[:,0,0])
        setattr(self, 'channel_water_surface_water__asymmetry_of_ald_of_chlorophyll',
                self.lstm_output['tau'].detach().numpy()[:,0,0])

        self.t = time + 1


    def forecast(self, lead_time=None):
        """
        Forecast for a particular lead time, which is the number of days into the future from time t.

        Parameters:
            lead_time (int): Number of days into the future to forecast.

        Returns:
            pd.DataFrame: A DataFrame containing forecasted values ('mu' and 'sd') for each time step up to the lead time.
        """
        # Dispatch to encoder-decoder forecast if applicable
        if self.model_type == 'encoder_decoder':
            return self.forecast_encoder_decoder(lead_time=lead_time)

        # Save the current states of the model's hidden and cell states,
        #  as well as the current time step and input vars
        saved_h_t = self.h_t.clone()
        saved_c_t = self.c_t.clone()
        saved_t = self.t
        reference_datetime = self.get_current_date()
        site_ids = self.site_ids[:,0,0]
        # Save the current random state in torch
        saved_rng_state = torch.get_rng_state()

        # If lead_time is not provided, use the default forecast horizon from the configuration
        if lead_time is None:
            lead_time = self.f_horizon
            print(f' setting forecat lead time to f_horizon from the config file: {lead_time}')

        # Check if the requested lead time extends beyond the available data,
        #  if it does, then throw an error
        input_val = self.get_value_ptr(self.x_vars[0])
        # Check if the value is a float or an array
        if isinstance(input_val, float):
            # If it's a float, the length is 1
            input_data_len = 1
        elif isinstance(input_val, (np.ndarray, list)):
            # If it's an array or list, get the length
            input_data_len = len(input_val)
        else:
            # Raise an error if it's an unexpected type
            raise TypeError("Unexpected type returned from get_value_ptr(). Expected a float or an array.")

        if (lead_time+1) > input_data_len:
            raise ValueError(f"The forecast lead time, {lead_time} days, extends beyond the length of data provided.\n Please use a forecast lead time of {input_data_len-1} or smaller.")

        # Save the current input variables so they aren't overwritten in the forecast loop
        saved_input_values = self.get_input_array(return_array=True)
        saved_input_values_clone = saved_input_values.copy() # NEED THIS COPY
        input_values_ndims = saved_input_values.ndim

        try:
            # Run the model until lead_time
            for i in range(lead_time + 1):
                cur_datetime = reference_datetime + np.timedelta64(i, "D")
                # Using yesterday's temp prediction as today's input
                if np.isfinite(self.lag_var_mean):
                    if i > 0:
                        # Get predictions from yesterday and insert into today's lagged chl value
                        q_50 = np.zeros(len(forecasted_mode))
                        q_05 = np.zeros(len(forecasted_mode))
                        q_95 = np.zeros(len(forecasted_mode))
                        for j in range(len(forecasted_mode)):
                            q_50[j] = ald_quantile(prob=np.array([0.5]), mu=forecasted_mode[j], sigma=forecasted_scale[j], p=forecasted_skewness[j])
                            q_05[j] = ald_quantile(prob=np.array([0.05]), mu=forecasted_mode[j], sigma=forecasted_scale[j], p=forecasted_skewness[j])
                            q_95[j] = ald_quantile(prob=np.array([0.95]), mu=forecasted_mode[j], sigma=forecasted_scale[j], p=forecasted_skewness[j])
                        saved_input_values[self.lag_var_pos, i, :] = q_50
                        # calculate PI_90 as uncertainty for predicted values
                        pi_90 = q_95 - q_05
                        saved_input_values[self.lag_var_uncertainty_pos, i, :] = pi_90
                        # TODO: add in lagged river discharge in a better way; just setting to first day for now
                        saved_input_values[9,i,:] = saved_input_values[9,0,:]

                    elif input_values_ndims == 3:
                        if any(np.isnan(saved_input_values[self.lag_var_pos, i, :])):
                            raise ValueError(f'Lagged chl input for day 0 is NaN for at least one site, please initialize with a prediction or observation of chl')
                    elif input_values_ndims == 2:
                        if np.isnan(saved_input_values[self.lag_var_pos, i]):
                            raise ValueError(f'Lagged chl input for day 0 is NaN, please initialize with a prediction or observation of chl')
                    elif input_values_ndims == 1:
                        if np.isnan(saved_input_values[self.lag_var_pos]):
                            raise ValueError(f'Lagged chl input for day 0 is NaN, please initialize with a prediction or observation of chl')

                # Set the values
                for k in range(len(self.x_vars)):
                    if input_values_ndims == 3:
                        self.set_value(self.x_vars[k], saved_input_values[k,i,:])
                    elif input_values_ndims == 2:
                        self.set_value(self.x_vars[k], saved_input_values[k,i])
                    elif input_values_ndims == 1:
                        self.set_value(self.x_vars[k], saved_input_values[k])

                # Update the model
                self.update()

                # Retrieve the forecasted values
                forecasted_mode = getattr(self, 'channel_water_surface_water__location_of_ald_of_chlorophyll', np.zeros(self.n_segs))
                forecasted_scale = getattr(self, 'channel_water_surface_water__scale_of_ald_of_chlorophyll', np.zeros(self.n_segs))
                forecasted_skewness = getattr(self, 'channel_water_surface_water__asymmetry_of_ald_of_chlorophyll', np.zeros(self.n_segs))

                if i == 0:
                    # Create a new DataFrame with the forecast data
                    forecasts = pd.DataFrame({
                        'reference_datetime': np.repeat(reference_datetime, self.n_segs),
                        'datetime': np.repeat(cur_datetime, self.n_segs),
                        'site_id': site_ids,
                        CSDMS_CHLA_LOC:   forecasted_mode,
                        CSDMS_CHLA_SCALE: forecasted_scale,
                        CSDMS_CHLA_ASYM:  forecasted_skewness
                    })
                else:
                    # Create a new DataFrame with the forecast data
                    new_forecasts = pd.DataFrame({
                        'reference_datetime': np.repeat(reference_datetime, self.n_segs),
                        'datetime': np.repeat(cur_datetime, self.n_segs),
                        'site_id': site_ids,
                        CSDMS_CHLA_LOC:   forecasted_mode,
                        CSDMS_CHLA_SCALE: forecasted_scale,
                        CSDMS_CHLA_ASYM:  forecasted_skewness
                    })
                    # Concatenate the new forecast with the existing DataFrame
                    forecasts = pd.concat([forecasts, new_forecasts], ignore_index=True)

        finally:
            # Restore the saved states to ensure the model's state is consistent after forecasting
            self.h_t = saved_h_t
            self.c_t = saved_c_t
            self.t = saved_t
            self.input_array = saved_input_values_clone
            self.set_values_from_input_array()
            # Restore the saved random state
            torch.set_rng_state(saved_rng_state)

        return forecasts

    def forecast_encoder_decoder(self, lead_time=None):
        """
        Forecast using the encoder-decoder architecture.

        This method builds encoder input from past observations and decoder input
        from GEFS forecasts, then runs the encoder-decoder model to produce
        forecasts for all lead times simultaneously.

        If autoregressive mode is enabled (decoder has chla_lagged), uses
        forward_autoregressive which feeds predictions back as lagged input.

        Parameters:
            lead_time (int): Maximum number of days into the future to forecast.
                            If None, uses decoder_seq_len from config.

        Returns:
            pd.DataFrame: A DataFrame containing forecasted values for each
                         time step up to the lead time.
        """
        reference_datetime = self.get_current_date()
        # Encoder-decoder inference is driven by forecast_data (there are no
        # per-date hidden states indexing self.site_ids, which filter_data_*
        # only sets for the standard LSTM path), so take the site order from the
        # forecast file the encoder/decoder inputs are built from.
        if hasattr(self, 'site_ids') and self.site_ids is not None:
            site_ids = self.site_ids[:, 0, 0]
        elif 'site_id' in self.forecast_data.coords:
            site_ids = np.asarray(self.forecast_data.site_id.values, dtype=object)
        else:
            site_ids = np.array(['unknown'])

        # Use decoder_seq_len as default lead_time
        if lead_time is None:
            lead_time = self.decoder_seq_len
            print(f"Setting forecast lead time to decoder_seq_len: {lead_time}")

        # Ensure lead_time doesn't exceed decoder_seq_len
        if lead_time > self.decoder_seq_len:
            print(f"Warning: lead_time ({lead_time}) exceeds decoder_seq_len ({self.decoder_seq_len}). "
                  f"Limiting to {self.decoder_seq_len}")
            lead_time = self.decoder_seq_len

        # Build encoder input (past observations)
        x_encoder = self._build_encoder_input()

        # Build decoder input (future forecasts with uncertainty)
        x_decoder = self._build_decoder_input(lead_time)

        # Check if autoregressive mode is enabled (decoder has chla_lagged)
        decoder_vars = self._get_decoder_vars()
        use_autoregressive = 'chla_lagged' in decoder_vars and getattr(self, 'decoder_autoregressive', False)

        # Run encoder-decoder model
        self.encoder_decoder_model.eval()
        with torch.no_grad():
            if use_autoregressive:
                # Get indices of lagged chla features in decoder
                chla_lagged_idx = decoder_vars.index('chla_lagged')
                chla_unc_idx = decoder_vars.index('chla_uncertainty_lagged')

                # Build scaling params for autoregressive mode
                ar_scaling_params = {
                    'target_mean': float(self.target_mean[0]),
                    'target_std': float(self.target_std[0]),
                    'decoder_chla_mean': float(self.decoder_mean[chla_lagged_idx]),
                    'decoder_chla_std': float(self.decoder_std[chla_lagged_idx]),
                    'decoder_unc_mean': float(self.decoder_mean[chla_unc_idx]),
                    'decoder_unc_std': float(self.decoder_std[chla_unc_idx])
                }

                output, (h_final, c_final) = self.encoder_decoder_model.forward_autoregressive(
                    x_encoder, x_decoder,
                    chla_lagged_idx=chla_lagged_idx,
                    chla_unc_idx=chla_unc_idx,
                    target_mean=ar_scaling_params['target_mean'],
                    target_std=ar_scaling_params['target_std'],
                    decoder_chla_mean=ar_scaling_params['decoder_chla_mean'],
                    decoder_chla_std=ar_scaling_params['decoder_chla_std'],
                    decoder_unc_mean=ar_scaling_params['decoder_unc_mean'],
                    decoder_unc_std=ar_scaling_params['decoder_unc_std']
                )
            else:
                output, (h_final, c_final) = self.encoder_decoder_model(x_encoder, x_decoder)

        # Extract forecasts from output
        # Output shape: (n_sites, decoder_seq_len, n_outputs)
        forecasted_mode = output['mu'].squeeze(-1).numpy()  # (n_sites, lead_time)
        forecasted_scale = output['b'].squeeze(-1).numpy()
        forecasted_skewness = output['tau'].squeeze(-1).numpy()

        # NOTE: When target_log_transformed=True, all parameters (mu, b, tau) stay in log-space
        # The back-transformation happens in R when computing quantiles:
        #   1. Compute quantiles in log-space using qALD(mu, b, tau)
        #   2. Back-transform quantiles: exp(q) - 0.01
        # This ensures proper uncertainty propagation through the nonlinear transform

        # Build DataFrame with all forecast lead times
        # lead_time=0 is the init_date (reference_datetime), lead_time=1 is +1 day, etc.
        forecast_rows = []
        for day in range(lead_time):
            cur_datetime = reference_datetime + np.timedelta64(day, "D")

            for site_idx, site_id in enumerate(site_ids):
                forecast_rows.append({
                    'reference_datetime': reference_datetime,
                    'datetime': cur_datetime,
                    'site_id': site_id,
                    'lead_time': day,
                    CSDMS_CHLA_LOC:   forecasted_mode[site_idx, day],
                    CSDMS_CHLA_SCALE: forecasted_scale[site_idx, day],
                    CSDMS_CHLA_ASYM:  forecasted_skewness[site_idx, day],
                    'log_transformed': getattr(self, 'target_log_transformed', False)
                })

        forecasts = pd.DataFrame(forecast_rows)

        return forecasts

    @staticmethod
    def _reorder(da, dims):
        """Transpose to ``dims``, ignoring dims the array doesn't have."""
        return da.transpose(*[d for d in dims if d in da.dims])

    # Features allowed to be partially missing (filled with the training mean,
    # matching how training handled them); all others must be complete.
    _GAPPY_FEATURES = ('chla_lagged', 'chla_uncertainty_lagged', LAST_CHLA_VAR)

    def _handle_missing_features(self, x, names, means, stage):
        """Substitute the training mean for whole features the driver source lacks.

        A *fully* NaN feature column means the met source has no equivalent for that
        variable (e.g. FLARE archives carry no ``total_cloud_cover_atmosphere``);
        filling it with its training mean puts it at 0 after scaling, which is the
        neutral value. A *partially* NaN column is a real data problem and still
        raises, so silent degradation stays impossible.
        """
        imputed, broken, gap_filled = [], [], []
        for i, name in enumerate(names):
            col = x[:, :, i]
            n_nan = int(np.isnan(col).sum())
            if n_nan == 0:
                continue
            if n_nan == col.size:
                x[:, :, i] = means[i]
                imputed.append(str(name))
            elif str(name) in self._GAPPY_FEATURES:
                # Observed-chla history can have gaps longer than the causal fill;
                # training filled those with the training mean, so do the same.
                x[:, :, i] = np.where(np.isnan(col), means[i], col)
                gap_filled.append(f"{name} ({n_nan} day(s))")
            else:
                broken.append(str(name))

        if broken:
            raise ValueError(
                f"{stage} input contains NaN for: {broken}. These features are only "
                "partially populated, which indicates a data problem rather than an "
                "unavailable variable -- check the forecast_data window."
            )
        if imputed:
            print(
                f"WARNING: {stage} feature(s) not available from the driver source; "
                f"filled with their training mean: {imputed}"
            )
        if gap_filled:
            print(f"NOTE: {stage} gaps filled with the training mean: {gap_filled}")
        return x

    def _build_encoder_input(self):
        """
        Build encoder input tensor from the observed lookback window.

        Selects exactly ``encoder_seq_len`` days ending the day *before* the
        reference date, matching the training samples (encoder = days
        R-encoder_seq_len .. R-1, decoder/targets start on R).

        Returns:
            torch.Tensor: Encoder input tensor of shape (n_sites, encoder_seq_len, n_encoder_feat)
        """
        # Get encoder variables from config
        encoder_vars = self._get_encoder_vars()

        # Build input array from forecast_data or encoder_history
        if self.encoder_history is not None:
            # Use accumulated history (already n_sites x encoder_seq_len x n_feat)
            x_encoder = np.asarray(self.encoder_history, dtype=np.float32)
            if hasattr(self, 'encoder_mean') and hasattr(self, 'encoder_std'):
                x_encoder = (x_encoder - self.encoder_mean) / (self.encoder_std + 1e-10)
            return torch.from_numpy(x_encoder).float()

        current_date = self.get_current_date()
        # Inclusive slice of exactly encoder_seq_len days ending the day before the
        # reference date.
        encoder_end_date = current_date - np.timedelta64(1, 'D')
        encoder_start_date = encoder_end_date - np.timedelta64(self.encoder_seq_len - 1, 'D')
        encoder_data = self.forecast_data.sel(
            time=slice(encoder_start_date, encoder_end_date)
        )

        n_sites = len(encoder_data.site_id)
        n_times = len(encoder_data.time)
        n_features = len(encoder_vars)

        if n_times != self.encoder_seq_len:
            raise ValueError(
                f"Encoder window has {n_times} day(s) but encoder_seq_len is "
                f"{self.encoder_seq_len} ({encoder_start_date} .. {encoder_end_date}). "
                "Check that forecast_data covers the full lookback window."
            )

        x_encoder = np.full((n_sites, n_times, n_features), np.nan, dtype=np.float32)

        for i, var in enumerate(encoder_vars):
            name = str(var)
            if name not in encoder_data:
                raise ValueError(f"Encoder variable '{name}' not found in forecast_data")

            var_data = self._reorder(encoder_data[name], ('site_id', 'time')).values
            if var_data.ndim == 2:
                x_encoder[:, :, i] = var_data
            elif var_data.ndim == 1:
                # Static variable - broadcast to all times
                x_encoder[:, :, i] = var_data[:, np.newaxis]
            else:
                raise ValueError(
                    f"Unexpected ndim ({var_data.ndim}) for encoder variable '{name}'"
                )

        x_encoder = self._handle_missing_features(
            x_encoder, encoder_vars, self.encoder_mean, 'Encoder'
        )

        # Scale using encoder scaling parameters
        if hasattr(self, 'encoder_mean') and hasattr(self, 'encoder_std'):
            x_encoder = (x_encoder - self.encoder_mean) / (self.encoder_std + 1e-10)

        return torch.from_numpy(x_encoder).float()

    def _lead_time_series(self, var, lead_time, n_sites):
        """``(n_sites, lead_time)`` values for a ``lead_time``-indexed variable.

        Returns None when the variable is absent or not lead-time indexed.
        """
        ds = self.forecast_data
        if var not in ds or 'lead_time' not in ds[var].dims:
            return None

        da = self._reorder(ds[var], ('site_id', 'lead_time'))
        vals = da.isel(lead_time=slice(0, lead_time)).values
        out = np.full((n_sites, lead_time), np.nan, dtype=np.float32)
        n = min(lead_time, vals.shape[1])
        out[:, :n] = vals[:, :n]
        return out

    def _static_series(self, var, lead_time, n_sites):
        """``(n_sites, lead_time)`` broadcast of a site-level variable, or None."""
        ds = self.forecast_data
        if var not in ds:
            return None
        base = ds[var]
        if 'time' in base.dims:
            base = base.isel(time=0)
        vals = np.asarray(self._reorder(base, ('site_id',)).values)
        if vals.ndim == 0:
            vals = np.full(n_sites, float(vals))
        return np.repeat(vals.reshape(n_sites, 1), lead_time, axis=1).astype(np.float32)

    def _build_decoder_input(self, lead_time):
        """
        Build decoder input tensor from the stored lead-time forecasts.

        The encoder-decoder ``forecast_data`` file holds each met variable twice: the
        encoder history on ``time`` and the future forecast on ``lead_time`` as
        ``{var}_forecast`` (plus ``{var}_pi90`` for spread). Static features are stored
        broadcast over ``time`` and are reused here.

        Parameters:
            lead_time (int): Number of forecast days

        Returns:
            torch.Tensor: Decoder input tensor of shape (n_sites, lead_time, n_decoder_feat)
        """
        decoder_vars = self._get_decoder_vars()
        n_sites = self.forecast_data.sizes['site_id']
        n_features = len(decoder_vars)

        x_decoder = np.full((n_sites, lead_time, n_features), np.nan, dtype=np.float32)

        for i, var in enumerate(decoder_vars):
            name = str(var)

            if name.endswith('_pi90'):
                col = self._lead_time_series(name, lead_time, n_sites)
                if col is None:
                    col = np.full((n_sites, lead_time), np.nan, dtype=np.float32)
                # Spread with no underlying forecast (e.g. FLARE carries no
                # total_cloud_cover_atmosphere) -> the feature's scaler mean, which
                # is what these columns held during training (they were identically
                # zero there). Any other value is outside the model's support.
                fill = float(self.decoder_mean[i]) if hasattr(self, 'decoder_mean') else 0.0
                x_decoder[:, :, i] = np.where(np.isnan(col), fill, col)
                continue

            if name == LAST_CHLA_VAR:
                # Latest observed chla at forecast time = chla_lagged on the last
                # encoder day (the day before the reference date), on every lead day.
                last_day = self.get_current_date() - np.timedelta64(1, 'D')
                if 'chla_lagged' in self.forecast_data:
                    vals = self._reorder(self.forecast_data['chla_lagged'].sel(time=last_day), ('site_id',)).values
                else:
                    vals = np.full(n_sites, np.nan)
                x_decoder[:, :, i] = np.repeat(np.asarray(vals, dtype=np.float32).reshape(n_sites, 1), lead_time, axis=1)
                continue

            col = self._lead_time_series(f"{name}_forecast", lead_time, n_sites)
            if col is None:
                # Static / non-forecast feature: reuse the site-level values.
                col = self._static_series(name, lead_time, n_sites)
            if col is None:
                raise ValueError(
                    f"Decoder variable '{name}' not found in forecast_data as "
                    f"'{name}_forecast' or as a site-level variable."
                )
            x_decoder[:, :, i] = col

        x_decoder = self._handle_missing_features(
            x_decoder, decoder_vars, self.decoder_mean, 'Decoder'
        )

        # Scale using decoder scaling parameters
        if hasattr(self, 'decoder_mean') and hasattr(self, 'decoder_std'):
            x_decoder = (x_decoder - self.decoder_mean) / (self.decoder_std + 1e-10)

        x_decoder = self._hold_untrained_inputs(x_decoder, decoder_vars)

        return torch.from_numpy(x_decoder).float()

    def _hold_untrained_inputs(self, x_decoder, decoder_vars):
        """Pin decoder inputs that never varied in training to their training value.

        A feature that was constant in the training data (e.g. ``*_pi90`` spread for a
        model trained on ``flare_s3_stage3`` drivers, or cloud cover when the driver
        source had none) has untrained weights, so feeding it real values at inference
        would push them through those weights. Such inputs are held at the (scaled)
        training constant instead. Static site features are skipped -- they are
        constant by design and identical at inference. Once the model is retrained on
        data where the feature varies, this is a no-op for it.
        """
        x_trn = getattr(self, 'x_decoder_trn', None)
        if x_trn is None:
            return x_decoder

        static = set(self.cfg_bmi.get('x_vars_static') or [])
        held = []
        for i, var in enumerate(decoder_vars):
            name = str(var)
            if name in static:
                continue
            col_trn = x_trn[..., i]
            lo, hi = np.nanmin(col_trn), np.nanmax(col_trn)
            if np.isclose(lo, hi):
                x_decoder[:, :, i] = lo
                held.append(name)

        if held:
            print(
                "WARNING: these decoder inputs were constant in training, so they are held "
                f"at their training value (retrain with data where they vary to use them): {held}"
            )
        return x_decoder

        held = []
        for i, var in enumerate(decoder_vars):
            name = str(var)
            if not name.endswith('_pi90'):
                continue
            col_trn = x_trn[..., i]
            lo, hi = np.nanmin(col_trn), np.nanmax(col_trn)
            if np.isclose(lo, hi):
                x_decoder[:, :, i] = lo
                held.append(name)

        if held:
            print(
                "WARNING: forecast spread was constant in training, so these decoder "
                f"inputs are held at their training value (retrain on stage2 to use them): {held}"
            )
        return x_decoder

    def _get_encoder_vars(self):
        """Get encoder variable names."""
        if hasattr(self, 'encoder_vars') and self.encoder_vars is not None:
            return list(self.encoder_vars)

        # Default: met vars + hydro vars + static vars
        encoder_vars = list(self.x_vars) + list(self.hydro_vars) + list(self.x_vars_static)
        return encoder_vars

    def _get_decoder_vars(self):
        """Get decoder variable names."""
        if hasattr(self, 'decoder_vars') and self.decoder_vars is not None:
            return list(self.decoder_vars)

        # Default: met vars + PI90 vars + optionally static vars
        decoder_vars = list(self.x_vars)
        # Add PI90 for each met variable
        for var in self.x_vars:
            decoder_vars.append(f"{var}_pi90")
        if getattr(self, 'decoder_include_static', True):
            decoder_vars = decoder_vars + list(self.x_vars_static)
        return decoder_vars

    def update_encoder_history(self, new_observations):
        """
        Update the encoder history buffer with new observations.

        This method maintains a rolling window of observations for the encoder.

        Parameters:
            new_observations (np.ndarray): New observations to add,
                shape (n_sites, 1, n_encoder_feat)
        """
        if self.encoder_history is None:
            # Initialize with zeros
            n_sites = new_observations.shape[0]
            self.encoder_history = np.zeros(
                (n_sites, self.encoder_seq_len, self.n_encoder_feat),
                dtype=np.float32
            )

        # Roll the history and add new observations
        self.encoder_history = np.roll(self.encoder_history, shift=-1, axis=1)
        self.encoder_history[:, -1, :] = new_observations[:, 0, :]

    def train_model(self):
        """Train the model using the specified training data.

        This function prepares the training data and trains the model using either pre-training or fine-tuning
        methods or both, depending on the configuration. It converts the training data from NumPy arrays to PyTorch
        tensors, initializes model parameters, and manages the training process including the application of
        different loss functions based on the model head type.

        The function also handles the creation of necessary directories for saving model weights and outputs
        the hidden states after training.

        For encoder-decoder models, it dispatches to train_model_encoder_decoder().

        Returns:
            None: This function does not return any value. It performs in-place updates to the model state
            and saves the resulting hidden states to specified output files.

        Raises:
            OSError: If the specified weights directory cannot be created or accessed.

        Notes:
            - The function supports different model heads (e.g., GMM, CMAL, UMAL, Regression) and adjusts
            the training procedure accordingly.
            - The model can be pre-trained and/or fine-tuned based on the configuration flags `self.pre_train`
            and `self.fine_tune`.
            - Dropout behavior is managed based on the `self.mc_dropout` flag to ensure correct training
            and evaluation modes.
        """
        # Never replace a previously trained model's weights, log or saved config.
        check_no_overwrite(
            [self.weights_file, self.log_file,
             os.path.join(self.train_dir, f'{self.model_id}_config.yml')],
            self.cfg_bmi,
        )

        # Dispatch to encoder-decoder training if model_type is encoder_decoder
        if self.model_type == 'encoder_decoder':
            self.train_model_encoder_decoder()
            return

        self.x_trn = torch.from_numpy(self.x_trn).float()
        self.x_val = torch.from_numpy(self.x_val).float()
        self.x_test = torch.from_numpy(self.x_test).float()
        self.x_all = torch.from_numpy(self.x_all).float()
        self.y_trn = torch.from_numpy(self.y_trn).float()
        self.y_val = torch.from_numpy(self.y_val).float()
        self.obs_trn = torch.from_numpy(self.obs_trn).float()
        self.obs_val = torch.from_numpy(self.obs_val).float()

        # self.obs_test = torch.from_numpy(self.obs_test).float()
        self.dist_mat_trn = torch.from_numpy(self.dist_mat_trn).float()
        self.dist_mat_val = torch.from_numpy(self.dist_mat_val).float()
        self.dist_mat_test = torch.from_numpy(self.dist_mat_test).float()
        self.start_h_trn = torch.from_numpy(self.start_h_trn).float()
        self.start_c_trn = torch.from_numpy(self.start_c_trn).float()
        self.start_h_val = torch.from_numpy(self.start_h_val).float()
        self.start_c_val = torch.from_numpy(self.start_c_val).float()
        self.start_h_test = torch.from_numpy(self.start_h_test).float()
        self.start_c_test = torch.from_numpy(self.start_c_test).float()
        self.start_h_all_dates = torch.from_numpy(self.start_h_all_dates).float()
        self.start_c_all_dates = torch.from_numpy(self.start_c_all_dates).float()

        self.umal_n_taus_train = [1, self.head_n_dist][self.umal_extend_batch]
        if self.head == 'GMM':
            if self.weight_loss:
                self.loss_fn = MaskedGMMLoss_weighted
            else:
                self.loss_fn = MaskedGMMLoss
        if self.head == 'CMAL':
            self.loss_fn = MaskedCMALLoss
        if self.head == 'UMAL':
            self.loss_fn = MaskedUMALLoss
            if self.umal_extend_batch == True:
                self.x_trn = self.x_trn.repeat(self.head_n_dist, 1, 1)
                self.y_trn = self.y_trn.repeat(self.head_n_dist, 1, 1)
                self.x_trn_fine = self.x_trn_fine.repeat(self.head_n_dist, 1, 1)
                self.obs_trn = self.obs_trn.repeat(self.head_n_dist, 1, 1)
        if self.head == 'Regression':
            self.loss_fn = rmse_masked

        if not os.path.exists(self.weights_dir):
            os.makedirs(self.weights_dir, exist_ok=True)

        if self.pre_train:
            self.pretrain_model = LSTMWithHead(self.n_feat,
                                               self.hidden_units,
                                               self.dist_mat_trn,
                                               self.dropout_rate,
                                               self.recurrent_dropout_rate,
                                               self.head,
                                               self.head_hidden_dim,
                                               self.head_n_dist)

            self.pretrain_model.train() # ensure that dropout layers are active
            train_torch(model=self.pretrain_model,
                        loss_function=self.loss_fn,
                        optimizer=torch.optim.Adam(self.pretrain_model.parameters(), lr = self.learn_rate_pre),
                        x_train=self.x_trn,
                        y_train=self.y_trn,
                        h_train=self.start_h_trn,
                        c_train=self.start_c_trn,
                        h_val=self.start_h_val,
                        c_val=self.start_c_val,
                        weighting_matrix_train=self.dist_mat_trn,
                        weighting_matrix_val=self.dist_mat_val,
                        batch_size=self.x_trn.shape[0],
                        max_epochs=self.n_epochs_pre,
                        head=self.head,
                        umal_extend_batch=self.umal_extend_batch,
                        umal_n_taus_train=self.umal_n_taus_train,
                        umal_tau_min=self.umal_tau_min,
                        umal_tau_max=self.umal_tau_max,
                        weight_loss=self.weight_loss,
                        weight_threshold=self.weight_threshold,
                        weight_value=self.weight_value,
                        early_stopping_patience=self.early_stopping_patience,
                        x_val=self.x_val,
                        y_val=self.y_val,
                        shuffle=False, # hard coding to False
                        weights_file=self.weights_file,
                        log_file=self.log_file,
                        device='cpu', # hard coding
                        keep_portion=None)

        if self.fine_tune:
            self.fine_tune_model = LSTMWithHead(self.n_feat,
                                                self.hidden_units,
                                                self.dist_mat_trn,
                                                self.dropout_rate,
                                                self.recurrent_dropout_rate,
                                                self.head,
                                                self.head_hidden_dim,
                                                self.head_n_dist)

            if self.pre_train:
                self.fine_tune_model.load_state_dict(torch.load(self.weights_file, weights_only=True))

            self.fine_tune_model.train() # ensure that dropout layers are active
            train_torch(model=self.fine_tune_model,
                        loss_function=self.loss_fn,
                        optimizer=torch.optim.Adam(self.fine_tune_model.parameters(), lr = self.learn_rate_fine),
                        x_train=self.x_trn,
                        y_train=self.obs_trn,
                        h_train=self.start_h_trn,
                        c_train=self.start_c_trn,
                        h_val=self.start_h_val,
                        c_val=self.start_c_val,
                        weighting_matrix_train=self.dist_mat_trn,
                        weighting_matrix_val=self.dist_mat_val,
                        batch_size=self.x_trn.shape[0],
                        max_epochs=self.n_epochs_fine,
                        head=self.head,
                        umal_extend_batch=self.umal_extend_batch,
                        umal_n_taus_train=self.umal_n_taus_train,
                        umal_tau_min=self.umal_tau_min,
                        umal_tau_max=self.umal_tau_max,
                        weight_loss=self.weight_loss,
                        weight_threshold=self.weight_threshold,
                        weight_value=self.weight_value,
                        early_stopping_patience=self.early_stopping_patience,
                        x_val=self.x_val,
                        y_val=self.y_val,
                        shuffle=False, # hard coding to False
                        weights_file=self.weights_file,
                        log_file=self.log_file,
                        device='cpu', # hard coding
                        keep_portion=None)

            print("Finished Training")
            self.fine_tune_model.load_state_dict(torch.load(self.weights_file, weights_only=True))

            # writing out training predictions and obs
            pred_train = predict_from_io_data(
                model = self.fine_tune_model,
                head = self.head,
                io_data = self.data_file,
                partition = "train",
                outfile = os.path.join(self.train_dir, (self.model_id + "_train_preds.feather")),
                log_vars = False,  # not used - chla in raw scale
                spatial_idx_name = self.spatial_idx_name,
                time_idx_name = self.time_idx_name
            )
            # writing out all dates predictions and obs
            pred_all_dates = predict_from_io_data(
                model = self.fine_tune_model,
                head = self.head,
                io_data = self.data_file,
                partition = "all_dates",
                outfile = os.path.join(self.train_dir, (self.model_id + "_all_dates_preds.feather")),
                log_vars = False,  # not used - chla in raw scale
                spatial_idx_name = self.spatial_idx_name,
                time_idx_name = self.time_idx_name
            )
            # writing out test predictions and obs
            pred_test = predict_from_io_data(
                model = self.fine_tune_model,
                head = self.head,
                io_data = self.data_file,
                partition = "test",
                outfile = os.path.join(self.train_dir, (self.model_id + "_test_preds.feather")),
                log_vars = False,  # not used - chla in raw scale
                spatial_idx_name = self.spatial_idx_name,
                time_idx_name = self.time_idx_name
            )
            cfg_out_file = os.path.join(self.train_dir, (self.model_id + "_config.yml"))
            with open(cfg_out_file, 'w') as f:
                yaml.dump(self.cfg_bmi, f, default_flow_style=False, indent=4)

        print("Done training")


    def finalize(self):
        """Finalize model"""
        self._model = None


    def initialize_forcings(self):
        """Initialize all forcings to zero.

        This function initializes all forcing variables to zero. It iterates through the list of forcing variable names
        and sets each variable to zero using the BMI standard naming convention. For BMI functions that require long variable
        names, they should be mapped to the model's short names before taking action.

        Note:
            A BMI-enabled model should not use long variable names internally. Instead, it should use convenient short names
            for internal processing.
        """
        print('Initializing all forcings to 0...')
        for forcing_name in self.x_vars:
            print('  forcing_name =', forcing_name)
            setattr(self, forcing_name, 0)

    #------------------------------------------------------------
    def get_scaler_values(self):
        """Calculate mean and standard deviation for the input variables.

        This function calculates the mean and standard deviation for the input variables and model outputs.
        It extracts the mean and standard deviation values from pre-calculated data and assigns them to
        corresponding attributes in the model.

        For standard LSTM models, sets input_mean and input_std from x_data_mean/x_data_sd.
        For encoder-decoder models, the scaling values are already set in _filter_encoder_decoder_data().
        """
        if self.model_type == 'encoder_decoder':
            # Encoder-decoder stores separate encoder/decoder scalers.
            # get_unscaled_values expects input_mean/input_std aligned to self.x_vars,
            # so build those from available per-feature scaler arrays.
            decoder_var_to_idx = {
                str(var): i for i, var in enumerate(getattr(self, 'decoder_vars', []))
            }
            encoder_var_to_idx = {
                str(var): i for i, var in enumerate(getattr(self, 'encoder_vars', []))
            }

            input_mean = []
            input_std = []
            for var in self.x_vars:
                var_name = str(var)
                if var_name in decoder_var_to_idx:
                    idx = decoder_var_to_idx[var_name]
                    input_mean.append(float(self.decoder_mean[idx]))
                    input_std.append(float(self.decoder_std[idx]))
                elif var_name in encoder_var_to_idx:
                    idx = encoder_var_to_idx[var_name]
                    input_mean.append(float(self.encoder_mean[idx]))
                    input_std.append(float(self.encoder_std[idx]))
                else:
                    # Fallback for missing variables; keeps unscale path usable.
                    input_mean.append(0.0)
                    input_std.append(1.0)

            self.input_mean = np.array(input_mean, dtype=np.float32)
            self.input_std = np.array(input_std, dtype=np.float32)
        else:
            # Standard LSTM model
            self.input_mean = self.x_data_mean
            self.input_std = self.x_data_sd

    #------------------------------------------------------------
    def get_unscaled_values(self, lead_time=0, vars='all') -> pd.DataFrame:
        """
        Get the unscaled input values for the model based on the current time and lead time.

        Parameters:
            lead_time (int): Number of days into the future to request unscaled data.
            vars (list): List of variable names that correspond to model inputs.

        Returns:
            pd.DataFrame: Unscaled input values for the specified lead time with columns as variable names.
        """
        cur_time = int(self.t)
        cur_date = self.get_current_date()
        end_time = cur_time + lead_time + 1 # python slicing is exclusive for the end index so need to add 1
        end_date = cur_date + np.timedelta64(lead_time, "D")

        if vars == 'all':
            vars = self.x_vars

        # Check if all requested variables are in x_vars
        missing_vars = [var for var in vars if var not in self.x_vars]
        if missing_vars:
            raise ValueError(f"The following variables are not in x_vars: {missing_vars}; please select from {self.x_vars}")

        # TODO: Testing GEFS Data
        # unscaled_data = self.x_test[0:self.n_segs, cur_time:end_time, :] * (self.input_std + 1e-10) + self.input_mean
        # # Create a DataFrame from the unscaled data
        # xr_out = xr.DataArray(unscaled_data , dims = ['site_id','time','variable'], coords={'site_id':self.site_ids[0:self.n_segs,0,0],'time': range(cur_time, end_time), 'variable':self.x_vars}).to_dataset(dim='variable')
        # xr_out = xr_out.transpose("time", "site_id")
        # xr_out = xr_out[vars]

        scaled_data = (
            self.forecast_data.sel(time = cur_date)
            # .sel(ensemble_member = 0) # TODO: only taking one ensemble right now
            # Takes median of ensemble for met variables
            # PI90 variables (if present) are broadcast across ensemble, so median just returns the PI90 value
            .quantile(0.5, dim='ensemble_member')
            .sel(lead_time = slice(np.timedelta64(0, "D"),
                                   np.timedelta64(lead_time, "D")))
        )
        mean_sd_xr = xr.Dataset({
            var: ('stat', np.array([self.input_mean[i], self.input_std[i]])) for i, var in enumerate(self.x_vars)
        })

        mean_sd_xr = mean_sd_xr.assign_coords(stat=['mean', 'std'])

        unscaled_data = scaled_data * (mean_sd_xr.sel(stat = 'std') + 1e-10) + mean_sd_xr.sel(stat = 'mean')

        xr_out = unscaled_data[vars]

        return xr_out

    #------------------------------------------------------------
    def create_scaled_input_tensor(self, VERBOSE=False):
        """Create a scaled input tensor for the LSTM model.

        This function creates a scaled input tensor for the LSTM model using the mean and standard deviation
        values of the input variables calculated previously. It iterates through the input variables, maps
        short variable names to long variable names, retrieves their values from the model, and appends them
        to an input list. The input values are then normalized and reshaped into a tensor suitable for input
        to the LSTM model.

        Args:
            VERBOSE (bool, optional): If True, print verbose information during tensor creation. Default is False.

        """
        self.get_input_array(VERBOSE)

        DEBUG = False
        if (VERBOSE):
            print('Normalizing the tensor...')
            print('  input_mean =', self.input_mean )
            print('  input_std  =', self.input_std )
            print()
        # Center and scale the input values for use in torch
        # adding small number in case there is a std of zero
        #  Final array shape should be [n_locations, n_time, n_features]
        if self.input_array.ndim == 2:
            # input array only one time step
            self.input_array = self.input_array[:,np.newaxis,:] # adding time dim
            self.input_array_scaled = ((self.input_array - self.input_mean[:,np.newaxis,np.newaxis]) / (self.input_std[:,np.newaxis,np.newaxis] + 1e-10))
            self.input_array_scaled = np.transpose(self.input_array_scaled, (2,1,0))
        elif self.input_array.ndim == 3:
            # multiple timesteps and site ids
            n_time = self.input_array.shape[1]
            self.input_array_scaled = ((self.input_array - self.input_mean[:,np.newaxis,np.newaxis]) / (self.input_std[:,np.newaxis,np.newaxis] + 1e-10))
            self.input_array_scaled = np.transpose(self.input_array_scaled, (2,1,0))

        if (DEBUG):
            print('### input_list =', self.input_list)
            print('### input_array =', self.input_array)
            print('### dtype(input_array) =', self.input_array.dtype )
            print('### type(input_array_scaled) =', type(self.input_array_scaled))
            print('### dtype(input_array_scaled) =', self.input_array_scaled.dtype )
            print()
        self.input_tensor = torch.from_numpy(self.input_array_scaled).float()

    #------------------------------------------------------------
    def get_input_array(self, VERBOSE=False, return_array=False):
        """
        Retrieve and compile input variables into a NumPy array.

        This function gathers input variables specified by the `x_vars` attribute,
        retrieves their corresponding values, and compiles them into a single
        NumPy array. It also includes debug output if the VERBOSE or DEBUG
        flags are enabled.

        The method assumes that the input variables are stored in the object's
        attributes and that they can be accessed via the `getattr()` function.
        It also normalizes the resulting array to ensure it is of type `float64`.

        Args:
            VERBOSE (bool, optional): If True, print verbose information during tensor creation. Default is False.

        Attributes:
            self.input_list (list): A list of the retrieved input values.
            self.input_array (np.ndarray): A NumPy array containing the input values.

        Returns:
            None: This method does not return a value but sets attributes
            `input_list` and `input_array` on the instance.
        """
        n_inputs = len(self.x_vars)
        self.input_list = []
        DEBUG = False
        for k in range(n_inputs):
            short_name = self.x_vars[k]
            long_name  = self._var_name_map_short_first[ short_name ]
            # vals = self.get_value( long_name )
            vals = getattr( self, short_name )

            self.input_list.append( vals )
            if (VERBOSE or DEBUG):
                print('  short_name =', short_name )
                print('  long_name  =', long_name )
                array = getattr( self, short_name )
                # array = self.get_value( long_name )
                print('  type       =', type(vals) )
                print('  vals       =', vals )

        #--------------------------------------------------------
        # W/o setting dtype here, it was "object_", and crashed
        #--------------------------------------------------------
        ## self.input_array = np.array( self.input_list )
        self.input_array = np.array( self.input_list, dtype='float64' )

        if return_array:
            return self.input_array


    #------------------------------------------------------------
    def set_values_from_input_array(self):
        """
        Set model variable values from the input array.

        This function iterates through the input variables specified by the
        `x_vars` attribute and assigns values from the `input_array`
        to each corresponding variable. The values are set using the
        `set_value` method.

        The method handles both 1-D and 2-D NumPy arrays.

        Attributes:
            self.input_array (np.ndarray): A 1-D or 2-D NumPy array containing
            the input values to be assigned to the model variables.
            self.x_vars (list): A list of variable names that correspond
            to the rows in `input_array`.

        Returns:
            None: This method does not return a value but updates the
            model variables with the values from `input_array`.
        """
        n_inputs = len(self.x_vars)

        # Check if input_array is 1-D
        if self.input_array.ndim == 1:
            if self.input_array.shape[0] != n_inputs:
                raise ValueError("The number of rows in input_array must match the number of variables in x_vars.")
            for k in range(n_inputs):
                short_name = self.x_vars[k]
                vals = self.input_array[k]
                self.set_value(short_name, vals)

        # If input_array is 2-D
        elif self.input_array.ndim == 2:
            if self.input_array.shape[0] != n_inputs:
                raise ValueError("The number of rows in input_array must match the number of variables in x_vars.")
            for k in range(n_inputs):
                short_name = self.x_vars[k]
                vals = self.input_array[k, :]
                self.set_value(short_name, vals)

        # If input_array is 3-D
        elif self.input_array.ndim == 3:
            if self.input_array.shape[0] != n_inputs:
                raise ValueError("The number of rows in input_array must match the number of variables in x_vars.")
            for k in range(n_inputs):
                short_name = self.x_vars[k]
                vals = self.input_array[k, :, :]
                self.set_value(short_name, vals)

        else:
            raise ValueError("input_array must be a 1-D, 2-D, or 3-D NumPy array.")

    #------------------------------------------------------------
    def get_value(self, var_name: str, dest: np.ndarray) -> np.ndarray:
        """
        Copy values for the named variable into the provided destination array.

        Parameters
        ----------
        var_name : str
            Name of variable as CSDMS Standard Name.
        dest : np.ndarray
            A numpy array into which to copy the variable values.
        Returns
        -------
        np.ndarray
            Copy of values.
        """
        dest[:] = self.get_value_ptr(var_name)
        return dest

    #-------------------------------------------------------------------
    def get_value_ptr(self, var_name: str) -> np.ndarray:
        """
        Get reference to values.

        Get the backing reference - i.e., the backing numpy array - for the given variable.

        Parameters
        ----------
        var_name : str
            Name of variable as CSDMS Standard Name.
        Returns
        -------
        np.ndarray
            Value array.
        """
        # We actually need this function to return the backing array, so bypass override of __getattribute__ (that
        # extracts scalar) and use the base implementation
        return super(bmi_lstm, self).__getattribute__(var_name)

    #-------------------------------------------------------------------
    def set_value(self, var_name: str, values):
        """Set model values.

        This function sets the values of a model variable specified by its CSDMS Standard Name.

        Parameters:
            var_name (str): Name of the variable as CSDMS Standard Name.
            values (np.ndarray or pd.Series or pd.DataFrame): Values to set for the variable.

        """
        # Ensure values is a NumPy scalar
        if isinstance(values, (pd.Series, pd.DataFrame)):
            values = values.to_numpy()  # Convert Pandas objects to NumPy array
        elif isinstance(values, xr.DataArray):
            values = values.to_numpy()

        if isinstance(values, np.ndarray):
            if values.size == 1:
                values = values.item()  # Get the scalar value from the array
            else:
                values = values
        elif isinstance(values, (int, float)):
            values = np.array(values).item()  # Convert to NumPy scalar

        setattr( self, var_name, values )

    #-------------------------------------------------------------------
    #-------------------------------------------------------------------
    # BMI: Variable Information Functions
    #-------------------------------------------------------------------
    #-------------------------------------------------------------------
    def get_var_name(self, long_var_name):
        """Get the short variable name corresponding to a long variable name.

        This function retrieves the short variable name corresponding to a given long variable name.
        It looks up the variable name in the model's internal mapping from long variable names to short
        variable names.

        Parameters:
            long_var_name (str): The long variable name to look up.

        Returns:
            str: The short variable name corresponding to the given long variable name.

        """
        return self._var_name_map_long_first[ long_var_name ]

    #-------------------------------------------------------------------
    def get_var_units(self, long_var_name):
        """Get the units of a variable specified by its long name.

        This function retrieves the units of a variable specified by its long name.
        It looks up the variable name in the model's internal mapping from long variable names to units.

        Parameters:
            long_var_name (str): The long variable name for which to retrieve units.

        Returns:
            str: The units of the variable specified by its long name.

        """
        return self._var_units_map[ long_var_name ]


    def get_training_configurations(self):
        """Retrieve model configurations from the BMI configuration file.

        This function retrieves various training configurations from the BMI configuration file and assigns them
        to corresponding attributes in the model.
        """
        self.root_dir = self.cfg_bmi.get('root_dir')
        self.model_type = str(self.cfg_bmi['model_type'])

        if self.root_dir is not None:
            self.train_dir = os.path.join(self.root_dir, self.cfg_bmi['train_dir'])
            self.forecast_data_file = os.path.join(self.root_dir, self.cfg_bmi['forecast_data_file'])
        else:
            self.train_dir = self.cfg_bmi['train_dir']
            self.forecast_data_file = self.cfg_bmi['forecast_data_file']

        # Get model_id: uses explicit value from config if set, otherwise auto-generates
        self.model_id = get_model_id(self.cfg_bmi)
        if self.model_id is None:
            raise ValueError("Failed to generate model_id. Check model_config.yml settings.")
        print(f"Using model_id: {self.model_id}")
        # Store the resolved model_id back in config for saving/logging
        self.cfg_bmi['model_id'] = self.model_id
        data_file_rel = os.path.join(training_data_dir(self.cfg_bmi), f'{self.model_id}.npz')
        self.data_file = os.path.join(self.root_dir, data_file_rel) if self.root_dir is not None else data_file_rel
        self.weights_dir = os.path.join(self.train_dir, f'{self.model_id}_wgts')
        self.weights_file = os.path.join(self.weights_dir, 'weights.pth')
        self.train_preds_file = os.path.join(self.train_dir, f'{self.model_id}_train_preds.parquet')
        self.log_file = os.path.join(self.train_dir, f'{self.model_id}_train_log.csv')
        self.out_h_file = os.path.join(self.train_dir, f'{self.model_id}_h.npy')
        self.out_c_file = os.path.join(self.train_dir, f'{self.model_id}_c.npy')
        self.test_preds_file = os.path.join(self.train_dir, f'{self.model_id}_test_preds.parquet')
        self.all_dates_preds_file = os.path.join(self.train_dir, f'{self.model_id}_all_dates_preds.parquet')

        # log_vars removed - chlorophyll kept in raw µg/L scale
        self.log_vars = []
        self.spatial_idx_name = str(self.cfg_bmi['spatial_idx_name'])
        self.time_idx_name = str(self.cfg_bmi['time_idx_name'])
        self.mc_dropout = bool(self.cfg_bmi['mc_dropout'])
        self.recurrent_dropout_rate = float(self.cfg_bmi['recurrent_dropout_rate'])
        self.dropout_rate = float(self.cfg_bmi['dropout_rate'])
        self.temp_obs_sd = float(self.cfg_bmi['temp_obs_sd'])
        self.h_sd = float(self.cfg_bmi['h_sd'])
        self.c_sd = float(self.cfg_bmi['c_sd'])
        self.hidden_units = int(self.cfg_bmi['hidden_units'])
        self.force_pos = bool(self.cfg_bmi['force_pos'])
        self.update_h = bool(self.cfg_bmi['update_h'])
        self.update_c = bool(self.cfg_bmi['update_c'])
        self.f_horizon = int(self.cfg_bmi['f_horizon'])
        self.head = str(self.cfg_bmi['head'])
        self.head_hidden_dim = int(self.cfg_bmi['head_hidden_units'])
        self.head_n_dist = int(self.cfg_bmi['head_n_distr'])
        self.weight_loss = bool(self.cfg_bmi['weight_loss'])
        self.weight_threshold = float(self.cfg_bmi['weight_threshold'])
        self.weight_value = float(self.cfg_bmi['weight_value'])
        self.produce_ensembles = bool(self.cfg_bmi['produce_ensembles'])
        self.n_samples = int(self.cfg_bmi['n_samples'])
        self.pre_train = bool(self.cfg_bmi['pre_train'])
        self.fine_tune = bool(self.cfg_bmi['fine_tune'])
        self.n_epochs_pre = int(self.cfg_bmi['n_epochs_pre'])
        self.n_epochs_fine = int(self.cfg_bmi['n_epochs_fine'])
        self.early_stopping_patience = int(self.cfg_bmi['early_stopping_patience'])
        self.gpu = int(self.cfg_bmi['gpu'])
        self.umal_extend_batch = bool(self.cfg_bmi['umal_extend_batch'])
        self.umal_tau_min = float(self.cfg_bmi['umal_tau_min'])
        self.umal_tau_max = float(self.cfg_bmi['umal_tau_max'])
        self.learn_rate_pre = float(self.cfg_bmi['learn_rate_pre'])
        self.learn_rate_fine = float(self.cfg_bmi['learn_rate_fine'])
        self.torch_seed = int(self.cfg_bmi['seed'])

        # Encoder-decoder architecture settings
        self.encoder_seq_len = int(self.cfg_bmi.get('encoder_seq_len', 365))
        self.decoder_seq_len = int(self.cfg_bmi.get('decoder_seq_len', 10))
        self.decoder_autoregressive = bool(self.cfg_bmi.get('decoder_autoregressive', True))
        self.decoder_include_static = bool(self.cfg_bmi.get('decoder_include_static', True))
        self.residual_state_transfer = bool(self.cfg_bmi.get('residual_state_transfer', False))
        self.use_obs_uncertainty = bool(self.cfg_bmi.get('use_obs_uncertainty_in_loss', False))
        self.n_mc_samples = int(self.cfg_bmi.get('obs_uncertainty_n_mc_samples', 50)) 

    def get_data(self, train = False):
        """Load and retrieve data from the data file.

        This function loads data from the specified data file and retrieves various data arrays and attributes
        needed for model training and evaluation. It assigns the retrieved data to corresponding attributes
        in the model.

        For encoder-decoder models, it loads data with separate encoder and decoder inputs.

        Parameters:
            train (bool): A flag indicating whether to load training data (True) or test data (False).
                        Default is False.

        Returns:
            None: This function does not return any value. It assigns the loaded data to the instance attributes.
        """
        data = np.load(self.data_file, allow_pickle=True)

        # For forecasting mode, load forecast data
        if not train or self.model_type != 'encoder_decoder':
            self.forecast_data = xr.load_dataset(
                filename_or_obj=self.forecast_data_file,
                engine="netcdf4",
                chunks=None,
                decode_timedelta=True)

        # Dispatch to appropriate data filter based on model type
        if self.model_type == 'encoder_decoder':
            self.filter_data_encoder_decoder(data, train)
        else:
            self.filter_data(data, train)

    def filter_data(self, data, train):
        """Filter and assign data based on the training flag.

        This function processes the loaded data to filter out the relevant variables based on whether the
        data is for training or testing. It retrieves lagged variable information and assigns the appropriate
        data arrays to the instance attributes.

        Parameters:
            data (dict): A dictionary containing the loaded data arrays and attributes.
            train (bool): A flag indicating whether the data is for training (True) or testing (False).

        Returns:
            None: This function does not return any value. It assigns the filtered data to the instance attributes.
        """
        vars = data['x_vars']
        lag_var_name = data['lag_var']

        if lag_var_name in vars:
            # get the new position of the lagged variable
            self.lag_var_pos = data['lag_var_pos'][0]
            self.lag_var_uncertainty_pos = data['lag_var_uncertainty_pos'][0]
            self.lag_var_mean = data['lag_var_mean']
            self.lag_var_sd = data['lag_var_std']
            self.lag_var_uncertainty_mean = data['lag_var_uncertainty_mean']
            self.lag_var_uncertainty_sd = data['lag_var_uncertainty_std']
            self.lag_days = self.cfg_bmi['lag_days']
        else:
            self.lag_var_mean = float('NaN')
            self.lag_var_sd = float('NaN')
            self.lag_var_uncertainty_mean = float('NaN')
            self.lag_var_uncertainty_sd = float('NaN')
            self.lag_var_pos = float('NaN')
            self.lag_var_uncertainty_pos = float('NaN')
            self.lag_days = float('NaN')

        self.x_trn = data['x_train']
        self.x_val = data['x_val']
        self.x_test = data['x_test']
        self.x_all = data['x_all_dates']
        self.x = data['x_all_dates']

        self.y_trn = data['pretrain_train']
        self.y_val = data['pretrain_val']
        self.y_test = data['pretrain_test']

        self.obs_trn = data['obs_train']
        self.obs_val = data['obs_val']
        self.obs_test = data['obs_test']
        self.obs_all = data['obs_all_dates']
        self.obs = data['obs_all_dates']

        self.x_vars = vars
        self.obs_vars = data['obs_vars']
        self.n_feat = self.x_trn.shape[2]

        self.dates_trn = data['times_train']
        self.dates_val = data['times_val']
        self.dates_test = data['times_test']
        self.dates_all = data['times_all_dates']

        self.dist_mat_trn = data['weighting_matrix_train']
        self.dist_mat_val = data['weighting_matrix_val']
        self.dist_mat_test = data['weighting_matrix_test']
        self.dist_mat_all = data['weighting_matrix_test']

        self.start_h_trn = data['h_train']
        self.start_c_trn = data['c_train']
        self.start_h_val = data['h_val']
        self.start_c_val = data['c_val']
        self.start_h_test = data['h_test']
        self.start_c_test = data['c_test']
        self.start_h_all_dates = data['h_all_dates']
        self.start_c_all_dates = data['c_all_dates']

        # select vars based on dictionary
        self.x_data_mean = data['x_mean']
        self.x_data_sd = data['x_std']
        self.obs_data_mean = data['obs_mean']
        self.obs_data_sd = data['obs_std']

        self.site_ids = data['ids_all_dates']
        self.n_segs = self.site_ids.shape[0]

    def filter_data_encoder_decoder(self, data, train):
        """Filter and assign data for encoder-decoder architecture.

        This function processes the loaded data to filter out the relevant variables
        for encoder-decoder training. It handles separate encoder and decoder input arrays.

        Parameters:
            data (dict): A dictionary containing the loaded data arrays and attributes.
            train (bool): A flag indicating whether the data is for training (True) or testing (False).

        Returns:
            None: This function does not return any value. It assigns the filtered data to the instance attributes.
        """
        # Encoder-decoder specific data
        self.x_encoder_trn = data['x_encoder_train']
        self.x_encoder_val = data['x_encoder_val']
        self.x_encoder_test = data.get('x_encoder_test', None)

        self.x_decoder_trn = data['x_decoder_train']
        self.x_decoder_val = data['x_decoder_val']
        self.x_decoder_test = data.get('x_decoder_test', None)

        self.y_trn = data['y_train']
        self.y_val = data['y_val']
        self.y_test = data.get('y_test', None)

        # Variable names
        self.encoder_vars = data['encoder_vars']
        self.decoder_vars = data['decoder_vars']
        self.target_vars = data['target_vars']

        # Keep legacy x_vars populated for shared helper paths used during forecast mode.
        self.x_vars = np.array(self.cfg_bmi.get('x_vars', list(self.encoder_vars)), dtype=object)

        # Scaling parameters
        self.encoder_mean = data['encoder_mean']
        self.encoder_std = data['encoder_std']
        self.decoder_mean = data['decoder_mean']
        self.decoder_std = data['decoder_std']
        self.target_mean = data['target_mean']
        self.target_std = data['target_std']

        # Metadata
        self.init_dates_trn  = data.get('init_dates_train', None)
        self.init_dates_val  = data.get('init_dates_val', None)
        self.init_dates_test = data.get('init_dates_test', None)
        self.site_ids_trn  = data.get('site_ids_train', None)
        self.site_ids_val  = data.get('site_ids_val', None)
        self.site_ids_test = data.get('site_ids_test', None)

        # Observation uncertainty (PI90) for loss function
        self.y_obs_pi90_trn = data.get('y_obs_pi90_train', None)
        self.y_obs_pi90_val = data.get('y_obs_pi90_val', None)

        # Log-transform flag for target variable
        # When True, targets are in log-space and predictions need back-transformation
        self.target_log_transformed = bool(data.get('target_log_transformed', False))
        if self.target_log_transformed:
            print("  Target variable is log-transformed (will back-transform predictions)")

        # Feature dimensions
        self.n_encoder_feat = self.x_encoder_trn.shape[-1]
        self.n_decoder_feat = self.x_decoder_trn.shape[-1]

        print(f"Loaded encoder-decoder data:")
        print(f"  Encoder train shape: {self.x_encoder_trn.shape}")
        print(f"  Decoder train shape: {self.x_decoder_trn.shape}")
        print(f"  Target train shape: {self.y_trn.shape}")
        print(f"  Encoder features: {list(self.encoder_vars)}")
        print(f"  Decoder features: {list(self.decoder_vars)}")


    def train_model_encoder_decoder(self):
        """Train the encoder-decoder model using the specified training data.

        This function prepares the training data and trains the encoder-decoder model.
        It converts the training data from NumPy arrays to PyTorch tensors, initializes
        model parameters, and manages the training process.

        Returns:
            None: This function does not return any value. It performs in-place updates
            to the model state and saves the resulting weights.
        """
        # Convert to PyTorch tensors
        self.x_encoder_trn = torch.from_numpy(self.x_encoder_trn).float()
        self.x_encoder_val = torch.from_numpy(self.x_encoder_val).float()
        self.x_decoder_trn = torch.from_numpy(self.x_decoder_trn).float()
        self.x_decoder_val = torch.from_numpy(self.x_decoder_val).float()
        self.y_trn = torch.from_numpy(self.y_trn).float()
        self.y_val = torch.from_numpy(self.y_val).float()

        # Convert observation uncertainty to tensors if available
        if self.y_obs_pi90_trn is not None:
            self.y_obs_pi90_trn = torch.from_numpy(self.y_obs_pi90_trn).float()
        if self.y_obs_pi90_val is not None:
            self.y_obs_pi90_val = torch.from_numpy(self.y_obs_pi90_val).float()

        # Validate observation uncertainty data is present if enabled
        if self.use_obs_uncertainty and self.y_obs_pi90_trn is None:
            raise ValueError(
                "use_obs_uncertainty_in_loss is True but y_obs_pi90_train not found in data file. "
                "Please re-run data prep (python get_data.py) to generate observation uncertainty data."
            )

        # Set up loss function based on head type
        if self.head == 'GMM':
            self.loss_fn = MaskedGMMLoss
        elif self.head == 'CMAL':
            self.loss_fn = MaskedCMALLoss
        elif self.head == 'UMAL':
            self.loss_fn = MaskedUMALLoss
        elif self.head == 'Regression':
            self.loss_fn = rmse_masked
        else:
            raise ValueError(f"Unknown head type: {self.head}")

        if not os.path.exists(self.weights_dir):
            os.makedirs(self.weights_dir, exist_ok=True)

        # Initialize encoder-decoder model
        self.encoder_decoder_model = EncoderDecoderLSTM(
            encoder_input_dim=self.n_encoder_feat,
            decoder_input_dim=self.n_decoder_feat,
            hidden_dim=self.hidden_units,
            head=self.head,
            head_hidden_dim=self.head_hidden_dim,
            head_n_dist=self.head_n_dist,
            dropout=self.dropout_rate,
            recur_dropout=self.recurrent_dropout_rate,
            residual_state_transfer=self.residual_state_transfer
        )

        print(f"Initialized EncoderDecoderLSTM:")
        print(f"  Encoder input dim: {self.n_encoder_feat}")
        print(f"  Decoder input dim: {self.n_decoder_feat}")
        print(f"  Hidden dim: {self.hidden_units}")
        print(f"  Head: {self.head}")

        # Check if autoregressive mode is enabled (decoder has chla_lagged AND config says to use AR)
        decoder_vars = list(self.decoder_vars) if hasattr(self, 'decoder_vars') else []
        use_autoregressive = 'chla_lagged' in decoder_vars and self.decoder_autoregressive
        chla_lagged_idx = None
        chla_unc_idx = None

        # Build autoregressive scaling parameters if needed
        ar_scaling_params = None
        if use_autoregressive:
            chla_lagged_idx = decoder_vars.index('chla_lagged')
            chla_unc_idx = decoder_vars.index('chla_uncertainty_lagged')
            print(f"  Autoregressive mode: enabled (chla_lagged_idx={chla_lagged_idx}, chla_unc_idx={chla_unc_idx})")

            # Build scaling params dict for transforming predictions to decoder input space
            ar_scaling_params = {
                'target_mean': float(self.target_mean[0]),
                'target_std': float(self.target_std[0]),
                'decoder_chla_mean': float(self.decoder_mean[chla_lagged_idx]),
                'decoder_chla_std': float(self.decoder_std[chla_lagged_idx]),
                'decoder_unc_mean': float(self.decoder_mean[chla_unc_idx]),
                'decoder_unc_std': float(self.decoder_std[chla_unc_idx])
            }
            print(f"  AR scaling: target(mean={ar_scaling_params['target_mean']:.4f}, std={ar_scaling_params['target_std']:.4f})")
            print(f"              decoder_chla(mean={ar_scaling_params['decoder_chla_mean']:.4f}, std={ar_scaling_params['decoder_chla_std']:.4f})")
            print(f"              decoder_unc(mean={ar_scaling_params['decoder_unc_mean']:.4f}, std={ar_scaling_params['decoder_unc_std']:.4f})")
        else:
            chla_lagged_idx = None
            chla_unc_idx = None
            print(f"  Autoregressive mode: disabled")

        self.encoder_decoder_model.train()

        # Train the model
        train_torch_encoder_decoder(
            model=self.encoder_decoder_model,
            loss_function=self.loss_fn,
            optimizer=torch.optim.Adam(self.encoder_decoder_model.parameters(), lr=self.learn_rate_fine),
            x_encoder_train=self.x_encoder_trn,
            x_decoder_train=self.x_decoder_trn,
            y_train=self.y_trn,
            x_encoder_val=self.x_encoder_val,
            x_decoder_val=self.x_decoder_val,
            y_val=self.y_val,
            y_obs_pi90_train=self.y_obs_pi90_trn,
            y_obs_pi90_val=self.y_obs_pi90_val,
            batch_size=32,  # TODO: make configurable
            max_epochs=self.n_epochs_fine,
            head=self.head,
            early_stopping_patience=self.early_stopping_patience,
            shuffle=True,
            weights_file=self.weights_file,
            log_file=self.log_file,
            device='gpu' if self.gpu else 'cpu',
            track_horizon_metrics=True,
            decoder_seq_len=self.decoder_seq_len,
            use_obs_uncertainty=self.use_obs_uncertainty,
            n_mc_samples=self.n_mc_samples,
            autoregressive=use_autoregressive,
            chla_lagged_idx=chla_lagged_idx,
            chla_unc_idx=chla_unc_idx,
            ar_scaling_params=ar_scaling_params
        )

        print("Finished Training Encoder-Decoder Model")

        # Load best weights
        self.encoder_decoder_model.load_state_dict(torch.load(self.weights_file, weights_only=True))

        # Generate predictions for training data
        print("Generating training predictions...")
        pred_train = predict_encoder_decoder_from_io_data(
            model=self.encoder_decoder_model,
            head=self.head,
            io_data=self.data_file,
            partition="train",
            outfile=os.path.join(self.train_dir, (self.model_id + "_train_preds.feather")),
            spatial_idx_name=self.spatial_idx_name,
            time_idx_name=self.time_idx_name,
        )

        # Generate predictions for validation data
        print("Generating validation predictions...")
        pred_val = predict_encoder_decoder_from_io_data(
            model=self.encoder_decoder_model,
            head=self.head,
            io_data=self.data_file,
            partition="val",
            outfile=os.path.join(self.train_dir, (self.model_id + "_val_preds.feather")),
            spatial_idx_name=self.spatial_idx_name,
            time_idx_name=self.time_idx_name,
        )

        # Generate predictions for test data if available
        data = np.load(self.data_file, allow_pickle=True)
        if 'x_encoder_test' in data.keys():
            print("Generating test predictions...")
            pred_test = predict_encoder_decoder_from_io_data(
                model=self.encoder_decoder_model,
                head=self.head,
                io_data=self.data_file,
                partition="test",
                outfile=os.path.join(self.train_dir, (self.model_id + "_test_preds.feather")),
                spatial_idx_name=self.spatial_idx_name,
                time_idx_name=self.time_idx_name,
            )

        # Save config
        cfg_out_file = os.path.join(self.train_dir, (self.model_id + "_config.yml"))
        with open(cfg_out_file, 'w') as f:
            yaml.dump(self.cfg_bmi, f, default_flow_style=False, indent=4)

        print("Done training encoder-decoder")


    def calc_expected_gradients(self, n_samples=200, target_output='mu',
                                decoder_lead_time=0, partition='all',
                                compute_encoder=True, compute_decoder=True,
                                outfile=None):
        """Dispatch expected gradients calculation to the model-appropriate method.

        Parameters
        ----------
        n_samples : int
            Number of Monte Carlo samples for the EG estimate. Default 200.
        target_output : str
            Which output to differentiate: 'mu', 'b', 'tau', 'median', or 'pi90'.
            'pi90' computes the 90% predictive interval width (Q95 - Q05).
        decoder_lead_time : int
            0-indexed decoder lead time (0=day-0, 9=day-10) used as the attribution
            target. For the LSTM dispatcher this is passed as ``temporal_focus``.
        partition : str
            Data partition for eval samples: 'train', 'val', 'test', or 'all'.
        compute_encoder : bool
            Compute EG for encoder (historical) inputs. Default True.
        compute_decoder : bool
            Compute EG for decoder (future forecast) inputs. Default True.
        outfile : str or None
            Optional path prefix for .feather export. Four files will be written:
            ``{outfile}_encoder.feather``, ``{outfile}_decoder.feather``,
            ``{outfile}_encoder_full.feather``, ``{outfile}_decoder_full.feather``.

        Returns
        -------
        tuple or pd.DataFrame
            For encoder-decoder models: ``(encoder_eg_df, decoder_eg_df)``.
            For standard LSTM: a single DataFrame.
        """
        if self.model_type == 'encoder_decoder':
            return self.expected_gradients_encoder_decoder(
                n_samples=n_samples,
                target_output=target_output,
                decoder_lead_time=decoder_lead_time,
                partition=partition,
                compute_encoder=compute_encoder,
                compute_decoder=compute_decoder,
                outfile=outfile,
            )
        else:
            return self.expected_gradients_lstm(
                n_samples=n_samples,
                temporal_focus=decoder_lead_time,
            )

    def calc_feature_importance(self):
        """
        Calculate feature importance based on the change in Negative Log-Likelihood (NLL).

        This method initializes the feature importance model and checks for the existence of the weights file.
        It then evaluates the model's performance on the original input data and calculates the NLL.
        For each feature variable, it generates a hypothesis by modifying the feature values, evaluates the model again,
        and computes the change in NLL (delta NLL). The results are stored in a Pandas DataFrame.

        The feature importance is determined by the impact of each feature on the model's performance,
        as measured by the change in NLL when the feature values are perturbed.

        Raises:
            FileNotFoundError: If the weights file specified by `self.weights_file` does not exist.

        Returns:
            None: The method stores the calculated feature importance in the instance variable `self.feat_importance`.
            For encoder-decoder models, also stores `self.feat_importance_encoder` and `self.feat_importance_decoder`.

        Attributes:
            self.feat_importance_model: An instance of the model (LSTMWithHead or EncoderDecoderLSTM).
            self.feat_importance: A Pandas DataFrame containing the feature names and their corresponding delta NLL values.
        """
        # Check if the weights file exists
        if not os.path.exists(self.weights_file):
            raise FileNotFoundError(f"The weights file '{self.weights_file}' does not exist.")

        # Select loss function based on head type
        if self.head == 'GMM':
            loss_fn = MaskedGMMLoss
        elif self.head == 'CMAL':
            loss_fn = MaskedCMALLoss
        elif self.head == 'UMAL':
            loss_fn = MaskedUMALLoss
        else:
            loss_fn = MaskedGMMLoss  # default fallback

        # Branch based on model type
        if self.model_type == 'encoder_decoder':
            self._calc_feature_importance_encoder_decoder(loss_fn)
        else:
            self._calc_feature_importance_lstm(loss_fn)

    def _calc_feature_importance_lstm(self, loss_fn):
        """Calculate feature importance for standard LSTM model."""
        self.feat_importance_model = LSTMWithHead(
            self.n_feat,
            self.hidden_units,
            self.dist_mat_all,
            self.dropout_rate,
            self.recurrent_dropout_rate,
            self.head,
            self.head_hidden_dim,
            self.head_n_dist
        )

        # Load model weights
        self.feat_importance_model.load_state_dict(torch.load(self.weights_file, weights_only=True))

        self.x_feat_importance = torch.from_numpy(self.x).float()
        self.obs_feat_importance = torch.from_numpy(self.obs).float()

        # Need to give some initial h and c
        self.start_h = torch.zeros(self.n_segs, self.hidden_units)
        self.start_c = torch.zeros(self.n_segs, self.hidden_units)

        if self.mc_dropout:
            pred_orig, (self.h, self.c) = self.feat_importance_model.train()(
                self.x_feat_importance, [self.start_h, self.start_c], self.dist_mat_all
            )
        else:
            pred_orig, (self.h, self.c) = self.feat_importance_model.eval()(
                self.x_feat_importance, [self.start_h, self.start_c], self.dist_mat_all
            )

        nll_orig = loss_fn(self.obs_feat_importance, pred_orig)
        fi_data = {
            'x_var': [],
            'delta_nll': []
        }
        for var in range(len(self.x_vars)):
            x_hypothesis = self.x_feat_importance.detach().clone()
            # Identify the 10th and 90th percentile of data distribution
            var_range = torch.quantile(x_hypothesis[:, :, var].flatten(), torch.tensor([.1, .9]))
            # Make random distribution within the range of the target variable
            x_hypothesis[:, :, var] = (var_range[0] - var_range[1]) * torch.rand_like(x_hypothesis[:, :, var]) + var_range[1]
            if self.mc_dropout:
                y_hypothesis, (self.h, self.c) = self.feat_importance_model.train()(
                    x_hypothesis, [self.start_h, self.start_c], self.dist_mat_all
                )
            else:
                y_hypothesis, (self.h, self.c) = self.feat_importance_model.eval()(
                    x_hypothesis, [self.start_h, self.start_c], self.dist_mat_all
                )

            nll_hypothesis = loss_fn(self.obs_feat_importance, y_hypothesis)
            delta_nll = nll_hypothesis - nll_orig
            # Append the feature name and delta_nll to the DataFrame
            fi_data['x_var'].append(self.x_vars[var])
            fi_data['delta_nll'].append(delta_nll.item())

        self.feat_importance = pd.DataFrame(fi_data)

    def _calc_feature_importance_encoder_decoder(self, loss_fn):
        """Calculate feature importance for encoder-decoder model.

        Computes feature importance separately for encoder and decoder inputs by
        perturbing each feature and measuring the change in NLL.
        """
        # Initialize encoder-decoder model
        self.feat_importance_model = EncoderDecoderLSTM(
            encoder_input_dim=self.n_encoder_feat,
            decoder_input_dim=self.n_decoder_feat,
            hidden_dim=self.hidden_units,
            head=self.head,
            head_hidden_dim=self.head_hidden_dim,
            head_n_dist=self.head_n_dist,
            dropout=self.dropout_rate,
            recur_dropout=self.recurrent_dropout_rate,
            residual_state_transfer=self.residual_state_transfer
        )

        # Load model weights with backwards compatibility for old key names
        state_dict = torch.load(self.weights_file, weights_only=True)
        key_mapping = {
            'hidden_transfer.0.weight': 'hidden_transfer_linear.weight',
            'hidden_transfer.0.bias': 'hidden_transfer_linear.bias',
        }
        for old_key, new_key in key_mapping.items():
            if old_key in state_dict and new_key not in state_dict:
                state_dict[new_key] = state_dict.pop(old_key)
        self.feat_importance_model.load_state_dict(state_dict)

        # Prepare validation data for feature importance calculation
        # Use validation data since it wasn't used during training
        if isinstance(self.x_encoder_val, np.ndarray):
            x_encoder = torch.from_numpy(self.x_encoder_val).float()
            x_decoder = torch.from_numpy(self.x_decoder_val).float()
            y_obs = torch.from_numpy(self.y_val).float()
        else:
            x_encoder = self.x_encoder_val.clone()
            x_decoder = self.x_decoder_val.clone()
            y_obs = self.y_val.clone()

        # Set model to eval mode (unless mc_dropout is enabled)
        if self.mc_dropout:
            self.feat_importance_model.train()
        else:
            self.feat_importance_model.eval()

        # Get baseline prediction (model returns (output, (h, c)), we only need output)
        with torch.no_grad():
            pred_orig, _ = self.feat_importance_model(x_encoder, x_decoder)
        nll_orig = loss_fn(y_obs, pred_orig)

        # Get variable names
        encoder_vars = list(self.encoder_vars) if hasattr(self, 'encoder_vars') else [f'enc_feat_{i}' for i in range(self.n_encoder_feat)]
        decoder_vars = list(self.decoder_vars) if hasattr(self, 'decoder_vars') else [f'dec_feat_{i}' for i in range(self.n_decoder_feat)]

        # Calculate encoder feature importance
        fi_encoder_data = {'x_var': [], 'delta_nll': [], 'source': []}
        for var_idx in range(self.n_encoder_feat):
            x_encoder_hyp = x_encoder.detach().clone()
            # Identify the 10th and 90th percentile of data distribution
            var_range = torch.quantile(x_encoder_hyp[:, :, var_idx].flatten(), torch.tensor([.1, .9]))
            # Make random distribution within the range
            x_encoder_hyp[:, :, var_idx] = (var_range[0] - var_range[1]) * torch.rand_like(x_encoder_hyp[:, :, var_idx]) + var_range[1]

            with torch.no_grad():
                pred_hyp, _ = self.feat_importance_model(x_encoder_hyp, x_decoder)
            nll_hyp = loss_fn(y_obs, pred_hyp)
            delta_nll = nll_hyp - nll_orig

            fi_encoder_data['x_var'].append(encoder_vars[var_idx])
            fi_encoder_data['delta_nll'].append(delta_nll.item())
            fi_encoder_data['source'].append('encoder')

        # Calculate decoder feature importance per lead time
        # For each feature and each lead time, perturb only that (feature, lead_time) combination
        decoder_seq_len = x_decoder.shape[1]
        fi_decoder_data = {'x_var': [], 'lead_time': [], 'delta_nll': [], 'source': []}

        # Calculate baseline NLL per lead time for comparison
        nll_orig_per_lead = []
        for lead_t in range(decoder_seq_len):
            nll_lead = loss_fn(y_obs[:, lead_t:lead_t+1, :], {
                k: v[:, lead_t:lead_t+1, :] for k, v in pred_orig.items()
            })
            nll_orig_per_lead.append(nll_lead)

        print(f"  Calculating decoder feature importance per lead time...")
        for var_idx in range(self.n_decoder_feat):
            for lead_t in range(decoder_seq_len):
                x_decoder_hyp = x_decoder.detach().clone()
                # Identify the 10th and 90th percentile of data distribution for this feature
                var_range = torch.quantile(x_decoder_hyp[:, :, var_idx].flatten(), torch.tensor([.1, .9]))
                # Perturb only at this specific lead time
                x_decoder_hyp[:, lead_t, var_idx] = (var_range[0] - var_range[1]) * torch.rand(x_decoder_hyp.shape[0]) + var_range[1]

                with torch.no_grad():
                    pred_hyp, _ = self.feat_importance_model(x_encoder, x_decoder_hyp)

                # Calculate NLL only for this lead time
                nll_hyp_lead = loss_fn(y_obs[:, lead_t:lead_t+1, :], {
                    k: v[:, lead_t:lead_t+1, :] for k, v in pred_hyp.items()
                })
                delta_nll = nll_hyp_lead - nll_orig_per_lead[lead_t]

                fi_decoder_data['x_var'].append(decoder_vars[var_idx])
                fi_decoder_data['lead_time'].append(lead_t)
                fi_decoder_data['delta_nll'].append(delta_nll.item())
                fi_decoder_data['source'].append('decoder')

            if (var_idx + 1) % 5 == 0:
                print(f"    Processed {var_idx + 1}/{self.n_decoder_feat} decoder features")

        # Store results
        self.feat_importance_encoder = pd.DataFrame(fi_encoder_data)
        self.feat_importance_decoder = pd.DataFrame(fi_decoder_data)

        # Combined DataFrame - encoder features get lead_time = NA
        fi_encoder_data['lead_time'] = [None] * len(fi_encoder_data['x_var'])
        combined_data = {
            'x_var': fi_encoder_data['x_var'] + fi_decoder_data['x_var'],
            'lead_time': fi_encoder_data['lead_time'] + fi_decoder_data['lead_time'],
            'delta_nll': fi_encoder_data['delta_nll'] + fi_decoder_data['delta_nll'],
            'source': fi_encoder_data['source'] + fi_decoder_data['source']
        }
        self.feat_importance = pd.DataFrame(combined_data)

        print(f"Feature importance calculated for encoder-decoder model:")
        print(f"  Encoder features: {len(self.feat_importance_encoder)}")
        print(f"  Decoder features: {len(self.feat_importance_decoder)} ({self.n_decoder_feat} features x {decoder_seq_len} lead times)")


    def expected_gradients_lstm(self, n_samples=200, temporal_focus=None):
        """
        Calculate expected gradients for the LSTM model based on input sequences and
        return a DataFrame containing the gradients.

        This method initializes an LSTM model, loads its weights, and
        computes gradients of the model's output with respect to its input features.
        It samples from the input data, computes gradients, and
        aggregates them over a specified number of samples.

        Parameters:
        ----------
        n_samples : int, optional
            The number of samples to draw for gradient computation. Default is 200.

        temporal_focus : int, optional
            If specified, the index of the time step to focus on when calculating gradients.
            If None, gradients for all time steps are computed. Default is None.

        Returns:
        -------
        pd.DataFrame
            A DataFrame containing the expected gradients for each feature, with dates corresponding to the input sequences.
            The DataFrame has the following columns:
            - 'date': The date corresponding to each observation.
            - Features specified in self.x_vars.

        Raises:
        ------
        FileNotFoundError
            If the weights file specified by self.weights_file does not exist.

        Notes:
        -----
        The function follows the methodology described in
        Erion et al. (2021) for calculating expected gradients.
        """
        self.eg_model = LSTMWithHead(self.n_feat,
                                    self.hidden_units,
                                    self.dist_mat_all,
                                    self.dropout_rate,
                                    self.recurrent_dropout_rate,
                                    self.head,
                                    self.head_hidden_dim,
                                    self.head_n_dist)
        # Check if the weights file exists
        if not os.path.exists(self.weights_file):
            raise FileNotFoundError(f"The weights file '{self.weights_file}' does not exist.")
        else:
            # load model
            self.eg_model.load_state_dict(torch.load(self.weights_file, weights_only=True))

        seq_length = 365
        x_eg = self.x
        x_eg = x_eg.squeeze(0)

        # Calculate the number of complete sequences
        n_sequences = x_eg.shape[0] // seq_length

        # Truncate to keep only complete sequences
        x_truncated = x_eg[:n_sequences * seq_length]
        dates_truncated = self.dates_all[0,:n_sequences * seq_length,0]

        # Reshape into sequences
        x_sequences = x_truncated.reshape(n_sequences, seq_length, self.n_feat)
        dates_sequences = dates_truncated.reshape(n_sequences, seq_length)

        x_sequences = torch.from_numpy(x_sequences).float()
        # need to give some initial h and c
        start_h = torch.zeros(n_sequences, self.hidden_units)
        start_c = torch.zeros(n_sequences, self.hidden_units)

        ## See Erion et al (2021) https://doi.org/10.1038/s42256-021-00343-w

        for k in range(n_samples):
            ## Sample a series from our data
            rand_seq = np.random.choice(n_sequences)
            baseline_x = x_sequences[rand_seq].to(torch.device('cpu'))

            ## Sample a random scale along the difference
            scale = np.random.uniform()

            ## Calculate the gradient of f(x) with regards to x
            x_diff = x_sequences - baseline_x
            curr_x = baseline_x + scale*x_diff
            if curr_x.requires_grad == False:
                curr_x.requires_grad = True
            self.eg_model.zero_grad()
            y, (h, c) = self.eg_model(curr_x, [start_h, start_c], self.dist_mat_all)
            y_mu = y['mu']

            ## Pull out the gradient
            if temporal_focus == None:
                gradients = torch.autograd.grad(y_mu[:, :, :], curr_x, torch.ones_like(y_mu[:, :, :]))
            else:
                gradients = torch.autograd.grad(y_mu[:, temporal_focus, :], curr_x, torch.ones_like(y_mu[:,temporal_focus, :]))

            if k == 0:
                expected_gradients = x_diff*gradients[0] * 1/n_samples
            else:
                expected_gradients = expected_gradients + ((x_diff*gradients[0]) * 1/n_samples)

        reshaped_output = expected_gradients.view(-1, expected_gradients.shape[-1])

        # Flatten the dates_sequences to match the reshaped output
        flattened_dates = dates_sequences.flatten()

        df = pd.DataFrame(reshaped_output.numpy(), columns=self.x_vars)  # Convert to DataFrame with feature names
        df['date'] = flattened_dates

        # Reorder columns to have 'date' as the first column
        df = df[['date'] + list(self.x_vars)]

        return(df)


    def expected_gradients_encoder_decoder(
        self,
        n_samples: int = 200,
        target_output: str = 'mu',
        decoder_lead_time: int = 0,
        partition: str = 'all',
        compute_encoder: bool = True,
        compute_decoder: bool = True,
        outfile: str = None,
    ):
        """Calculate expected gradients for the encoder-decoder model.

        Implements the expected gradients attribution method from Erion et al. (2021)
        for the encoder-decoder LSTM architecture. Attributions are computed
        separately for encoder (historical) inputs and decoder (future forecast)
        inputs via Monte Carlo averaging over randomly sampled baselines.

        For each evaluation sample x and baseline X' drawn from the training
        distribution, the attribution for feature i is::

            φᵢ(x) = E_{X'~D, α~U(0,1)} [
                ∂F(X' + α(x - X')) / ∂(X' + α(x - X'))ᵢ  ×  (xᵢ - X'ᵢ)
            ]

        This is estimated by averaging over ``n_samples`` (baseline, α) pairs.

        Bloom-period filtering is not done here. Instead, compute attributions
        for the full partition and join observations by init_date + site_id in R
        to filter to any chlorophyll threshold post-hoc using the *_full.feather
        output files.

        Parameters
        ----------
        n_samples : int
            Number of Monte Carlo samples for the EG estimate. Default 200.
        target_output : str
            Which model output to differentiate: 'mu', 'b', 'tau', 'median',
            or 'pi90'. 'median' computes the ALD median (Q50) from mu/b/tau.
            'pi90' computes the 90% predictive interval width (Q95 - Q05),
            capturing the joint effect of both scale and asymmetry on forecast
            uncertainty width.
        decoder_lead_time : int
            0-indexed decoder lead time to use as the attribution target
            (0 = day-0, 9 = day-10). Default 0.
        partition : str
            Data partition for eval samples: 'train', 'val', 'test', or 'all'.
            'test' restricts eval samples to the held-out test period, matching
            the partition used to compute all reported forecast metrics.
        compute_encoder : bool
            Whether to compute EG for encoder (historical) inputs. Default True.
        compute_decoder : bool
            Whether to compute EG for decoder (future forecast) inputs. Default True.
        outfile : str or None
            Optional path prefix for .feather export. Writes four files:
            ``{outfile}_encoder.feather``, ``{outfile}_decoder.feather``,
            ``{outfile}_encoder_full.feather``, ``{outfile}_decoder_full.feather``.

        Returns
        -------
        tuple of pd.DataFrame
            ``(encoder_eg_df, decoder_eg_df)`` where each DataFrame contains
            mean expected gradients averaged across eval samples.
            - ``encoder_eg_df``: columns ['relative_day'] + encoder_vars,
              with relative_day=0 being the most recent day.
            - ``decoder_eg_df``: columns ['lead_time'] + decoder_vars.

        Notes
        -----
        Results are stored on self after the call:
            - self.encoder_eg  : np.ndarray (n_eval, enc_seq_len, n_enc_feat)
            - self.decoder_eg  : np.ndarray (n_eval, dec_seq_len, n_dec_feat)
            - self.encoder_eg_init_dates : init dates for the eval samples
            - self.encoder_eg_df, self.decoder_eg_df : mean DataFrames (returned)
            - self.encoder_eg_full_df : per-sample encoder DataFrame with
              columns [site_id, init_date, relative_day, ...encoder_vars]
            - self.decoder_eg_full_df : per-sample decoder DataFrame with
              columns [site_id, init_date, lead_time, ...decoder_vars]

        When ``outfile`` is set, four files are written:
            - ``{outfile}_encoder.feather`` : mean attributions (for plots)
            - ``{outfile}_decoder.feather`` : mean attributions (for plots)
            - ``{outfile}_encoder_full.feather`` : per-sample attributions
              with init_date and site_id for seasonal/temporal analysis
            - ``{outfile}_decoder_full.feather`` : per-sample attributions
              with init_date and site_id for seasonal/temporal analysis

        References
        ----------
        Erion et al. (2021). Improving performance of deep learning models with
        axiomatic attribution priors and expected gradients.
        https://doi.org/10.1038/s42256-021-00343-w
        """
        if not os.path.exists(self.weights_file):
            raise FileNotFoundError(
                f"Weights file '{self.weights_file}' does not exist."
            )

        # --- Build model ---
        eg_model = EncoderDecoderLSTM(
            encoder_input_dim=self.n_encoder_feat,
            decoder_input_dim=self.n_decoder_feat,
            hidden_dim=self.hidden_units,
            head=self.head,
            head_hidden_dim=self.head_hidden_dim,
            head_n_dist=self.head_n_dist,
            dropout=self.dropout_rate,
            recur_dropout=self.recurrent_dropout_rate,
            residual_state_transfer=self.residual_state_transfer,
        )
        state_dict = torch.load(self.weights_file, weights_only=True)
        key_mapping = {
            'hidden_transfer.0.weight': 'hidden_transfer_linear.weight',
            'hidden_transfer.0.bias': 'hidden_transfer_linear.bias',
        }
        for old_key, new_key in key_mapping.items():
            if old_key in state_dict and new_key not in state_dict:
                state_dict[new_key] = state_dict.pop(old_key)
        eg_model.load_state_dict(state_dict)
        eg_model.eval()

        # --- Assemble eval-partition data (numpy arrays) ---
        def _to_numpy(arr):
            return arr.numpy() if isinstance(arr, torch.Tensor) else arr

        x_enc_trn = _to_numpy(self.x_encoder_trn)
        x_dec_trn = _to_numpy(self.x_decoder_trn)
        y_trn     = _to_numpy(self.y_trn)
        x_enc_val = _to_numpy(self.x_encoder_val)
        x_dec_val = _to_numpy(self.x_decoder_val)
        y_val     = _to_numpy(self.y_val)

        if partition == 'train':
            x_enc_eval, x_dec_eval, y_eval = x_enc_trn, x_dec_trn, y_trn
            init_dates = self.init_dates_trn
            site_ids   = self.site_ids_trn
        elif partition == 'val':
            x_enc_eval, x_dec_eval, y_eval = x_enc_val, x_dec_val, y_val
            init_dates = self.init_dates_val
            site_ids   = self.site_ids_val
        elif partition == 'test':
            if self.x_encoder_test is None:
                raise ValueError("No test data found in the .npz file.")
            x_enc_eval = _to_numpy(self.x_encoder_test)
            x_dec_eval = _to_numpy(self.x_decoder_test)
            y_eval     = _to_numpy(self.y_test)
            init_dates = self.init_dates_test
            site_ids   = self.site_ids_test
        else:  # 'all'
            x_enc_eval = np.concatenate([x_enc_trn, x_enc_val], axis=0)
            x_dec_eval = np.concatenate([x_dec_trn, x_dec_val], axis=0)
            y_eval     = np.concatenate([y_trn, y_val], axis=0)
            init_dates = (
                np.concatenate([self.init_dates_trn, self.init_dates_val])
                if self.init_dates_trn is not None and self.init_dates_val is not None
                else None
            )
            site_ids = (
                np.concatenate([self.site_ids_trn, self.site_ids_val])
                if self.site_ids_trn is not None and self.site_ids_val is not None
                else None
            )

        # Reference distribution always covers all available data
        x_enc_all = np.concatenate([x_enc_trn, x_enc_val], axis=0)
        x_dec_all = np.concatenate([x_dec_trn, x_dec_val], axis=0)
        n_all = len(x_enc_all)

        # --- Optional bloom filter ---
        eval_idx = np.arange(len(x_enc_eval))
        n_eval = len(eval_idx)
        print(f"Computing expected gradients for {n_eval} eval samples, "
              f"n_samples={n_samples}, target='{target_output}', "
              f"lead_time={decoder_lead_time}")

        x_enc_eval_t = torch.from_numpy(x_enc_eval[eval_idx]).float()  # (n_eval, enc_seq, enc_feat)
        x_dec_eval_t = torch.from_numpy(x_dec_eval[eval_idx]).float()  # (n_eval, dec_seq, dec_feat)

        enc_eg = torch.zeros_like(x_enc_eval_t)  # accumulated EG for encoder
        dec_eg = torch.zeros_like(x_dec_eval_t)  # accumulated EG for decoder

        for k in range(n_samples):
            ref_idx = np.random.randint(n_all)
            alpha = float(np.random.uniform())

            x_enc_ref = torch.from_numpy(x_enc_all[ref_idx:ref_idx + 1]).float()  # (1, enc_seq, enc_feat)
            x_dec_ref = torch.from_numpy(x_dec_all[ref_idx:ref_idx + 1]).float()  # (1, dec_seq, dec_feat)

            enc_diff = x_enc_eval_t - x_enc_ref  # broadcasts to (n_eval, enc_seq, enc_feat)
            dec_diff = x_dec_eval_t - x_dec_ref  # broadcasts to (n_eval, dec_seq, dec_feat)

            x_enc_interp = (x_enc_ref + alpha * enc_diff).detach().requires_grad_(True)
            x_dec_interp = (x_dec_ref + alpha * dec_diff).detach().requires_grad_(True)

            eg_model.zero_grad()
            output, _ = eg_model(x_enc_interp, x_dec_interp)

            if target_output == 'median':
                target = ald_quantile_torch(0.5, output['mu'], output['b'], output['tau'])
            elif target_output == 'pi90':
                # PI90 = Q95 - Q05: captures joint effect of scale (b) and
                # asymmetry (tau) on interval width, both differentiable via
                # ald_quantile_torch so gradients flow back to inputs normally.
                target = ald_pi90_torch(output['mu'], output['b'], output['tau'])
            else:
                target = output[target_output]

            # target shape: (n_eval, dec_seq_len, n_dist) — select the lead time
            target_at_lead = target[:, decoder_lead_time, :]  # (n_eval, n_dist)

            grad_inputs = []
            if compute_encoder:
                grad_inputs.append(x_enc_interp)
            if compute_decoder:
                grad_inputs.append(x_dec_interp)

            grads = torch.autograd.grad(
                outputs=target_at_lead,
                inputs=grad_inputs,
                grad_outputs=torch.ones_like(target_at_lead),
                retain_graph=False,
            )

            grad_idx = 0
            if compute_encoder:
                enc_eg = enc_eg + (enc_diff * grads[grad_idx]) / n_samples
                grad_idx += 1
            if compute_decoder:
                dec_eg = dec_eg + (dec_diff * grads[grad_idx]) / n_samples

            if (k + 1) % 50 == 0:
                print(f"  EG sample {k + 1}/{n_samples}")

        # --- Build output DataFrames ---
        enc_eg_np = enc_eg.detach().numpy()  # (n_eval, enc_seq_len, n_enc_feat)
        dec_eg_np = dec_eg.detach().numpy()  # (n_eval, dec_seq_len, n_dec_feat)

        encoder_vars = list(self.encoder_vars) if hasattr(self, 'encoder_vars') else \
            [f'enc_feat_{i}' for i in range(self.n_encoder_feat)]
        decoder_vars = list(self.decoder_vars) if hasattr(self, 'decoder_vars') else \
            [f'dec_feat_{i}' for i in range(self.n_decoder_feat)]

        eval_init_dates = init_dates[eval_idx] if init_dates is not None else None
        eval_site_ids   = site_ids[eval_idx]   if site_ids   is not None else None

        # Encoder: relative_day=0 is most recent; -(enc_seq_len-1) is oldest
        enc_eg_mean = enc_eg_np.mean(axis=0)  # (enc_seq_len, n_enc_feat)
        relative_days = np.arange(-(self.encoder_seq_len - 1), 1)
        encoder_eg_df = pd.DataFrame(enc_eg_mean, columns=encoder_vars)
        encoder_eg_df.insert(0, 'relative_day', relative_days)

        # Decoder: lead_time 0..dec_seq_len-1
        dec_eg_mean = dec_eg_np.mean(axis=0)  # (dec_seq_len, n_dec_feat)
        decoder_eg_df = pd.DataFrame(dec_eg_mean, columns=decoder_vars)
        decoder_eg_df.insert(0, 'lead_time', np.arange(self.decoder_seq_len))

        # --- Per-sample full DataFrames (for temporal binning in R) ---
        # Encoder full: (n_eval * enc_seq_len) rows; one row per (sample, time step)
        enc_reshaped = enc_eg_np.reshape(n_eval * self.encoder_seq_len, self.n_encoder_feat)
        encoder_eg_full_df = pd.DataFrame(enc_reshaped, columns=encoder_vars)
        encoder_eg_full_df.insert(0, 'relative_day', np.tile(relative_days, n_eval))
        if eval_init_dates is not None:
            encoder_eg_full_df.insert(0, 'init_date', np.repeat(eval_init_dates, self.encoder_seq_len))
        if eval_site_ids is not None:
            encoder_eg_full_df.insert(0, 'site_id', np.repeat(eval_site_ids, self.encoder_seq_len))

        # Decoder full: (n_eval * dec_seq_len) rows; one row per (sample, lead time)
        dec_reshaped = dec_eg_np.reshape(n_eval * self.decoder_seq_len, self.n_decoder_feat)
        decoder_eg_full_df = pd.DataFrame(dec_reshaped, columns=decoder_vars)
        decoder_eg_full_df.insert(0, 'lead_time', np.tile(np.arange(self.decoder_seq_len), n_eval))
        if eval_init_dates is not None:
            decoder_eg_full_df.insert(0, 'init_date', np.repeat(eval_init_dates, self.decoder_seq_len))
        if eval_site_ids is not None:
            decoder_eg_full_df.insert(0, 'site_id', np.repeat(eval_site_ids, self.decoder_seq_len))

        # --- Store raw arrays on self ---
        self.encoder_eg = enc_eg_np
        self.decoder_eg = dec_eg_np
        self.encoder_eg_init_dates = eval_init_dates
        self.encoder_eg_df = encoder_eg_df
        self.decoder_eg_df = decoder_eg_df
        self.encoder_eg_full_df = encoder_eg_full_df
        self.decoder_eg_full_df = decoder_eg_full_df

        # --- Optional feather export ---
        if outfile is not None:
            # Mean (used by existing plot pipeline)
            encoder_eg_df.to_feather(f"{outfile}_encoder.feather")
            decoder_eg_df.to_feather(f"{outfile}_decoder.feather")
            # Full per-sample (for temporal binning / seasonal analysis)
            encoder_eg_full_df.to_feather(f"{outfile}_encoder_full.feather")
            decoder_eg_full_df.to_feather(f"{outfile}_decoder_full.feather")
            print(f"EG results saved to {outfile}_encoder.feather and "
                  f"{outfile}_decoder.feather")
            print(f"Full per-sample EG saved to {outfile}_encoder_full.feather and "
                  f"{outfile}_decoder_full.feather")

        print(f"Expected gradients complete: encoder shape {enc_eg_np.shape}, "
              f"decoder shape {dec_eg_np.shape}")
        return encoder_eg_df, decoder_eg_df


    ### functions not implemented but needed for BMI class
    def get_grid_edge_count(self, grid):
        raise NotImplementedError("get_grid_edge_count")

    def get_component_name(self):
        """Name of the component."""
        return self._name

    def get_current_time(self):
        return self.t

    def get_current_date(self):
        # Encoder-decoder inference: the reference date (first forecast day), which
        # follows the last encoder day on forecast_data.time.
        if getattr(self, 'reference_date', None) is not None:
            return self.reference_date
        return self.forecast_data.time.isel(time = int(self.t)).values

    def get_end_time(self):
        return self._end_time

    def get_end_date(self):
        return self.forecast_data.time.isel(time = -1).values

    def get_grid_edge_count(self, grid):
        raise NotImplementedError("get_grid_edge_count")

    def get_grid_edge_nodes(self, grid):
        raise NotImplementedError("get_grid_edge_nodes")

    def get_grid_face_count(self, grid):
        raise NotImplementedError("get_grid_face_count")

    def get_grid_face_edges(self, grid):
        raise NotImplementedError("get_grid_face_edges")

    def get_grid_face_nodes(self, grid):
        raise NotImplementedError("get_grid_face_nodes")

    def get_grid_node_count(self, grid):
        raise NotImplementedError("get_grid_node_count")

    def get_grid_nodes_per_face(self, grid):
        raise NotImplementedError("get_grid_nodes_per_face")

    def get_grid_origin(self, grid):
        raise NotImplementedError("get_grid_origin")

    def get_grid_rank(self, grid):
        raise NotImplementedError("get_grid_rank")

    def get_grid_shape(self, grid):
        raise NotImplementedError("get_grid_shape")

    def get_grid_size(self, grid):
        raise NotImplementedError("get_grid_size")

    def get_grid_spacing(self, grid):
        raise NotImplementedError("get_grid_spacing")

    def get_grid_type(self, grid):
        raise NotImplementedError("get_grid_type")

    def get_grid_x(self, grid):
        raise NotImplementedError("get_grid_x")

    def get_grid_y(self, grid):
        raise NotImplementedError("get_grid_y")

    def get_grid_z(self, grid):
        raise NotImplementedError("get_grid_z")

    def get_input_item_count(self):
        """Get number of input variables."""
        return len(self._input_var_names)

    def get_input_var_names(self):
        """Get names of input variables."""
        return self._input_var_names

    def get_output_item_count(self):
        """Get number of output variables."""
        return len(self._output_var_names)

    def get_output_var_names(self):
        """Get names of output variables."""
        return self._output_var_names

    def get_start_time(self):
        return self._start_time

    def get_time_step(self):
        return self._time_step_size

    def get_time_units(self):
        return self._time_units

    def get_value_at_indices(self, var, indices):
        raise NotImplementedError("get_value_at_indices")

    def get_var_grid(self, var):
        raise NotImplementedError("get_var_grid")

    def get_var_itemsize(self, var):
        raise NotImplementedError("get_var_itemsize")

    #------------------------------------------------------------
    def get_var_location(self, name):
        # Note: all vars have location node but check if its in names list first
        if name in (self._output_var_names + self._input_var_names):
            return self._var_loc

    def get_var_nbytes(self, var):
        raise NotImplementedError("get_var_nbytes")

    #-------------------------------------------------------------------
    def get_var_type(self, long_var_name):
        """Get the data type of a variable specified by its long name.

        This function retrieves the data type of a variable specified by its long name.

        Parameters:
            long_var_name (str): The long variable name for which to retrieve the data type.

        Returns:
            str: The data type of the variable.

        """
        return self.get_value_ptr(long_var_name).dtype.name

    def set_value_at_indices(self, var, indices, value):
        raise NotImplementedError("set_value_at_indices")

    #------------------------------------------------------------
    #------------------------------------------------------------
    #-- Utility functions
    #------------------------------------------------------------
    #------------------------------------------------------------

    def _parse_config(self, cfg):
        """Parse configuration settings.

        This function parses configuration settings provided in a dictionary format. It converts path strings
        to `Path` objects, and converts date strings to pandas `DatetimeIndex` objects.

        Parameters:
            cfg (dict): A dictionary containing configuration settings.

        Returns:
            dict: A dictionary with parsed configuration settings.

        """
        for key, val in cfg.items():
            # convert all path strings to PosixPath objects
            if any([key.endswith(x) for x in ['_dir', '_path', '_file', '_files']]):
                if (val is not None) and (val != "None"):
                    if isinstance(val, list):
                        temp_list = []
                        for element in val:
                            if (USE_PATH):
                                temp_list.append( Path(element) )
                            else:
                                temp_list.append( element )  # (SDP)
                        cfg[key] = temp_list
                    else:
                        if (USE_PATH):
                            cfg[key] = Path( val )
                        else:
                            cfg[key] = val  # (SDP)
                else:
                    cfg[key] = None

            # convert Dates to pandas Datetime indexs
            elif key.endswith('_date'):
                if isinstance(val, list):
                    temp_list = []
                    for elem in val:
                        temp_list.append(pd.to_datetime(elem, format='%Y-%m-%d'))
                    cfg[key] = temp_list
                else:
                    cfg[key] = pd.to_datetime(val, format='%Y-%m-%d')

            else:
                pass

        # Add more config parsing if necessary
        return cfg

