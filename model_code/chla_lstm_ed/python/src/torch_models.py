import torch
import torch.nn as nn
import torch.jit as jit
from torch import Tensor
from typing import List, Tuple
from torch.nn import Parameter
from typing import Dict


# Simple LSTM made from scratch
#   Credit to / code modified from - https://towardsdatascience.com/building-a-lstm-by-hand-on-pytorch-59c02a4ec091
#   Associated github repo - https://github.com/piEsposito/pytorch-lstm-by-hand
class LSTM_v1(nn.Module):
    def __init__(self, input_dim, hidden_dim, adj_matrix, recur_dropout = 0, dropout = 0):
        super().__init__()
        
        self.input_dim = input_dim
        self.hidden_size = hidden_dim
        # See the file "neuralnet_math_README.md" in the root directory for
        # equations and implementation details
        self.weight_ih = nn.Parameter(torch.Tensor(input_dim, hidden_dim * 4))
        self.weight_hh = nn.Parameter(torch.Tensor(hidden_dim, hidden_dim * 4))
        self.bias = nn.Parameter(torch.Tensor(hidden_dim * 4))
        self.init_weights()
        
        self.dropout = nn.Dropout(dropout)
        self.recur_dropout = nn.Dropout(recur_dropout)
    
    def init_weights(self):
        for p in self.parameters():
            if p.data.ndimension() >= 2:
                nn.init.xavier_uniform_(p.data)
            else:
                nn.init.zeros_(p.data)
        
    def forward(self, x, init_states = None, adj_matrix = None):
        """Assumes x is of shape (batch, sequence, feature)"""
        bs, seq_sz, _ = x.size()
        hidden_seq = []
        if init_states is None:
            h_t, c_t = (torch.zeros(bs, self.hidden_size).to(x.device), 
                        torch.zeros(bs, self.hidden_size).to(x.device))
        else:
            h_t, c_t = init_states
        
        x = self.dropout(x)
        HS = self.hidden_size
        for t in range(seq_sz):
            x_t = x[:, t, :]
            # batch the computations into a single matrix multiplication
            gates = x_t @ self.weight_ih + h_t @ self.weight_hh + self.bias
            i_t, f_t, g_t, o_t = (
                torch.sigmoid(gates[:, :HS]), # input
                torch.sigmoid(gates[:, HS:HS*2]), # forget
                torch.tanh(gates[:, HS*2:HS*3]),
                torch.sigmoid(gates[:, HS*3:]), # output
            )
            c_t = f_t * c_t + i_t * self.recur_dropout(g_t)
            h_t = o_t * torch.tanh(c_t)
            hidden_seq.append(h_t.unsqueeze(1))
        hidden_seq = torch.cat(hidden_seq, dim= 1)
        return hidden_seq, (h_t, c_t)
      
      
class RGrN_v1(nn.Module):
    def __init__(self, input_dim, hidden_dim, adj_matrix, recur_dropout = 0, dropout = 0):
        super().__init__()
        
        # New stuff
        self.A = adj_matrix # torch.from_numpy(adj_matrix).float() # provided at initialization
        # parameters for mapping graph/spatial data
        self.weight_q = nn.Parameter(torch.Tensor(hidden_dim, hidden_dim))
        self.bias_q = nn.Parameter(torch.Tensor(hidden_dim))
        
        self.input_dim = input_dim
        self.hidden_size = hidden_dim
        self.weight_ih = nn.Parameter(torch.Tensor(input_dim, hidden_dim * 4))
        self.weight_hh = nn.Parameter(torch.Tensor(hidden_dim, hidden_dim * 4))
        self.bias = nn.Parameter(torch.Tensor(hidden_dim * 4))
        self.init_weights()
        
        self.dropout = nn.Dropout(dropout)
        self.recur_dropout = nn.Dropout(recur_dropout)
    
    def init_weights(self):
        for p in self.parameters():
            if p.data.ndimension() >= 2:
                nn.init.xavier_uniform_(p.data)
            else:
                nn.init.zeros_(p.data)
        
    def forward(self, x, init_states = None, adj_matrix = None):
        """Assumes x is of shape (batch, sequence, feature)"""
        bs, seq_sz, _ = x.size()
        hidden_seq = []
        if init_states is None:
            h_t, c_t = (torch.zeros(bs, self.hidden_size).to(x.device), 
                        torch.zeros(bs, self.hidden_size).to(x.device))
        else:
            h_t, c_t = init_states
        
        x = self.dropout(x)
        HS = self.hidden_size
        for t in range(seq_sz):
            x_t = x[:, t, :]
            # batch the computations into a single matrix multiplication
            gates = x_t @ self.weight_ih + h_t @ self.weight_hh + self.bias
            i_t, f_t, g_t, o_t = (
                torch.sigmoid(gates[:, :HS]), # input
                torch.sigmoid(gates[:, HS:HS*2]), # forget
                torch.tanh(gates[:, HS*2:HS*3]),
                torch.sigmoid(gates[:, HS*3:]), # output
            )
            q_t = torch.tanh(h_t @ self.weight_q + self.bias_q)
            if adj_matrix == None:
                c_t = f_t * (c_t + self.A @ q_t) + i_t * self.recur_dropout(g_t)
            # Option to use a different adjacency matrix on forward pass
            else:
                c_t = f_t * (c_t + adj_matrix @ q_t) + i_t * self.recur_dropout(g_t)
            h_t = o_t * torch.tanh(c_t)
            hidden_seq.append(h_t.unsqueeze(1))
        hidden_seq = torch.cat(hidden_seq, dim= 1)
        return hidden_seq, (h_t, c_t)
      

# Credit to / code modified from https://github.com/neuralhydrology/neuralhydrology
class GMM(nn.Module):
    """Gaussian Mixture Density Network
    A mixture density network with Gaussian distribution as components. Good references are [#]_ and [#]_. The latter 
    one forms the basis for our implementation. As such, we also use two layers in the head to provide it with 
    additional flexibility, and exponential activation for the variance estimates and a softmax for weights.  
    Parameters
    ----------
    n_in : int
        Number of input neurons.
    n_out : int
        Number of output neurons. Corresponds to 3 times the number of components.
    n_hidden : int
        Size of the hidden layer.
    
    References
    ----------
    .. [#] C. M. Bishop: Mixture density networks. 1994.
    .. [#] D. Ha: Mixture density networks with tensorflow. blog.otoro.net, 
           URL: http://blog.otoro.net/2015/11/24/mixture-density-networks-with-tensorflow, 2015.
    """

    def __init__(self, n_in: int, n_out: int, n_hidden: int = 100):
        super(GMM, self).__init__()
        self.fc1 = nn.Linear(n_in, n_hidden)
        self.fc2 = nn.Linear(n_hidden, n_out)
        self._eps = 1e-5

    def forward(self, x: torch.Tensor) -> Dict[str, torch.Tensor]:
        """Perform a GMM head forward pass.
        Parameters
        ----------
        x : torch.Tensor
            Output of the previous model part. It provides the basic latent variables to compute the GMM components.
        Returns
        -------
        Dict[str, torch.Tensor]
            Dictionary containing mixture parameters and weights; where the key 'mu' stores the means, the key
            'sigma' the variances, and the key 'pi' the weights.
        """
        h = torch.relu(self.fc1(x))
        h = self.fc2(h)

        # split output into mu, sigma and weights
        mu, sigma, pi = h.chunk(3, dim=-1)

        return {'mu': mu, 'sigma': torch.exp(sigma) + self._eps, 'pi': torch.softmax(pi, dim=-1)}
      
      
# Credit to / code modified from https://github.com/neuralhydrology/neuralhydrology 
class UMAL(nn.Module):
    """Uncountable Mixture of Asymmetric Laplacians.
    An implicit approximation to the mixture density network with Laplace distributions which does not require to
    pre-specify the number of components. An additional hidden layer is used to provide the head more expressiveness.
    General details about UMAL can be found in [#]_. A major difference between Brando's implementation 
    and NH's/ours is the binding-function for the scale-parameter (b). The scale needs to be lower-bound. The original UMAL 
    implementation uses an elu-based binding. In our experiment however, this produced under-confident predictions
    (too large variances). We therefore opted for a tailor-made binding-function that limits the scale from below and 
    above using a sigmoid. It is very likely that this needs to be adapted for non-normalized outputs.   
    Parameters
    ----------
    n_in : int
        Number of input neurons.
    n_out : int
        Number of output neurons. Corresponds to 2 times the output-size, since the scale parameters are also predicted.
    n_hidden : int
        Size of the hidden layer.
    References
    ----------
    .. [#] A. Brando, J. A. Rodriguez, J. Vitria, and A. R. Munoz: Modelling heterogeneous distributions 
        with an Uncountable Mixture of Asymmetric Laplacians. Advances in Neural Information Processing Systems, 
        pp. 8838-8848, 2019.
    """

    def __init__(self, n_in: int, n_out: int, n_hidden: int = 100):
        super(UMAL, self).__init__()
        self.fc1 = nn.Linear(n_in, n_hidden)
        self.fc2 = nn.Linear(n_hidden, n_out)
        self._upper_bound_scale = 0.5  # this parameter found empirical by testing UMAL for a limited set of basins
        self._eps = 1e-5

    def forward(self, x: torch.Tensor) -> Dict[str, torch.Tensor]:
        """Perform a UMAL head forward pass.
        Parameters
        ----------
        x : torch.Tensor
            Output of the previous model part. It provides the basic latent variables to compute the UMAL components.
        Returns
        -------
        Dict[str, torch.Tensor]
            Dictionary containing the means ('mu') and scale parameters ('b') to parametrize the asymmetric Laplacians.
        """
        h = torch.relu(self.fc1(x))
        h = self.fc2(h)

        m_latent, b_latent = h.chunk(2, dim=-1)

        # enforce properties on component parameters and weights:
        m = m_latent  # no restrictions (depending on setting m>0 might be useful)
        b = self._upper_bound_scale * torch.sigmoid(b_latent) + self._eps  # bind scale from two sides.
        return {'mu': m, 'b': b}
      
 
# Credit to / code modified from https://github.com/neuralhydrology/neuralhydrology     
class CMAL(nn.Module):
    """Countable Mixture of Asymmetric Laplacians.
    An mixture density network with Laplace distributions as components.
    The CMAL-head uses an additional hidden layer to give it more expressiveness (same as the GMM-head).
    CMAL is better suited for many hydrological settings as it handles asymmetries with more ease. However, it is also
    more brittle than GMM and can more often throw exceptions. Details for CMAL can be found in [#]_.
    Parameters
    ----------
    n_in : int
        Number of input neurons.
    n_out : int
        Number of output neurons. Corresponds to 4 times the number of components.
    n_hidden : int
        Size of the hidden layer.
        
    References
    ----------
    .. [#] D.Klotz, F. Kratzert, M. Gauch, A. K. Sampson, G. Klambauer, S. Hochreiter, and G. Nearing: 
        Uncertainty Estimation with Deep Learning for Rainfall-Runoff Modelling. arXiv preprint arXiv:2012.14295, 2020.
    """

    def __init__(self, n_in: int, n_out: int, n_hidden: int = 100):
        super(CMAL, self).__init__()
        self.fc1 = nn.Linear(n_in, n_hidden)
        self.fc2 = nn.Linear(n_hidden, n_out)

        self._softplus = torch.nn.Softplus(2)
        self._eps = 1e-5

    def forward(self, x: torch.Tensor) -> Dict[str, torch.Tensor]:
        """Perform a CMAL head forward pass.
        Parameters
        ----------
        x : torch.Tensor
            Output of the previous model part. It provides the basic latent variables to compute the CMAL components.
        Returns
        -------
        Dict[str, torch.Tensor]
            Dictionary, containing the mixture component parameters and weights; where the key 'mu'stores the means,
            the key 'b' the scale parameters, the key 'tau' the skewness parameters, and the key 'pi' the weights).
        """
        h = torch.relu(self.fc1(x))
        h = self.fc2(h)
        
        m_latent, b_latent, t_latent, p_latent = h.chunk(4, dim=-1)

        # enforce properties on component parameters and weights:
        m = m_latent  # no restrictions (depending on setting m>0 might be useful)
        b = self._softplus(b_latent) + self._eps  # scale > 0 (softplus was working good in tests)
        t = (1 - self._eps) * torch.sigmoid(t_latent) + self._eps  # 0 > tau > 1
        p = (1 - self._eps) * torch.softmax(p_latent, dim=-1) + self._eps  # sum(pi) = 1 & pi > 0

        return {'mu': m, 'b': b, 'tau': t, 'pi': p}
      

# Credit to / code modified from https://github.com/neuralhydrology/neuralhydrology
class Regression(nn.Module):
    """Single-layer regression head with different output activations.
    
    Parameters
    ----------
    n_in : int
        Number of input neurons.
    n_out : int
        Number of output neurons.
    """

    def __init__(self, n_in: int, n_out: int):
        super(Regression, self).__init__()

        # TODO: Add multi-layer support
        self.fc = nn.Linear(n_in, n_out)

    def forward(self, x: torch.Tensor) -> Dict[str, torch.Tensor]:
        """Perform a forward pass on the Regression head.
        
        Parameters
        ----------
        x : torch.Tensor
        Returns
        -------
        Dict[str, torch.Tensor]
            Dictionary containing the model predictions in the 'y_hat' key.
        """
        return {'y_hat': self.fc(x)}
     

class LSTMWithHead(nn.Module):
    def __init__(self, input_dim, lstm_hidden_dim, adj_matrix, dropout, recur_dropout,
                 head, head_hidden_dim, head_n_dist):
        super().__init__()
        self.lstm_layer = LSTM_v1(input_dim = input_dim,
                                 hidden_dim = lstm_hidden_dim, 
                                 adj_matrix = adj_matrix, 
                                 recur_dropout = recur_dropout, 
                                 dropout = dropout)
        assert(head in ['GMM', 'CMAL', 'UMAL', 'Regression'])
        if head == 'GMM':
            self.head_layer = GMM(n_in = lstm_hidden_dim,
                                  n_hidden = head_hidden_dim,
                                  n_out = 3*head_n_dist)
        if head == 'CMAL':
            self.head_layer = CMAL(n_in = lstm_hidden_dim,
                                   n_hidden = head_hidden_dim,
                                   n_out = 4*head_n_dist)
        if head == 'UMAL':
            self.head_layer = UMAL(n_in = lstm_hidden_dim,
                                   n_hidden = head_hidden_dim,
                                   n_out = 2*head_n_dist)
        if head == 'Regression':
            self.head_layer = Regression(n_in = lstm_hidden_dim,
                                         n_out = 1)
        
    def forward(self, x, init_states = None, adj_matrix = None):
        lstm_out, (h, c) = self.lstm_layer(x, init_states, adj_matrix)
        out = self.head_layer(lstm_out)
        return out, (h, c)



class RGrNWithHead(nn.Module):
    def __init__(self, input_dim, lstm_hidden_dim, adj_matrix, dropout, recur_dropout,
                 head, head_hidden_dim, head_n_dist):
        super().__init__()
        self.rgcn_layer = RGrN_v1(input_dim = input_dim,
                                  hidden_dim = lstm_hidden_dim,
                                  adj_matrix = adj_matrix,
                                  recur_dropout = recur_dropout,
                                  dropout = dropout)
        assert(head in ['GMM', 'CMAL', 'UMAL', 'Regression'])
        if head == 'GMM':
            self.head_layer = GMM(n_in = lstm_hidden_dim,
                                  n_hidden = head_hidden_dim,
                                  n_out = 3*head_n_dist)
        if head == 'CMAL':
            self.head_layer = CMAL(n_in = lstm_hidden_dim,
                                   n_hidden = head_hidden_dim,
                                   n_out = 4*head_n_dist)
        if head == 'UMAL':
            self.head_layer = UMAL(n_in = lstm_hidden_dim,
                                   n_hidden = head_hidden_dim,
                                   n_out = 2*head_n_dist)
        if head == 'Regression':
            self.head_layer = Regression(n_in = lstm_hidden_dim,
                                         n_out = 1)

    def forward(self, x, init_states = None, adj_matrix = None):
        lstm_out, (h, c) = self.rgcn_layer(x, init_states, adj_matrix)
        out = self.head_layer(lstm_out)
        return out, (h, c)


def ald_quantile_torch(prob: float, mu: torch.Tensor, b: torch.Tensor, tau: torch.Tensor) -> torch.Tensor:
    """
    Asymmetric Laplace Distribution quantile function (PyTorch version).

    Parameters
    ----------
    prob : float
        Probability value (e.g., 0.05 for Q05, 0.95 for Q95)
    mu : torch.Tensor
        Location parameter
    b : torch.Tensor
        Scale parameter (must be > 0)
    tau : torch.Tensor
        Skewness/asymmetry parameter (must be in (0, 1))

    Returns
    -------
    torch.Tensor
        Quantile values with same shape as mu, b, tau
    """
    # Numerical stability
    eps = 1e-10

    # Clamp tau to avoid log(0)
    tau_safe = torch.clamp(tau, eps, 1 - eps)

    # ALD quantile formula
    q = torch.where(
        prob < tau_safe,
        mu + (b * torch.log(torch.tensor(prob) / tau_safe)) / (1 - tau_safe),
        mu - (b * torch.log(torch.tensor(1 - prob) / (1 - tau_safe))) / tau_safe
    )
    return q


def ald_pi90_torch(mu: torch.Tensor, b: torch.Tensor, tau: torch.Tensor) -> torch.Tensor:
    """
    Calculate PI90 (90% prediction interval width) for Asymmetric Laplace Distribution.

    PI90 = Q95 - Q05

    Parameters
    ----------
    mu : torch.Tensor
        Location parameter
    b : torch.Tensor
        Scale parameter
    tau : torch.Tensor
        Skewness/asymmetry parameter

    Returns
    -------
    torch.Tensor
        PI90 values with same shape as inputs
    """
    q05 = ald_quantile_torch(0.05, mu, b, tau)
    q95 = ald_quantile_torch(0.95, mu, b, tau)
    return q95 - q05


class EncoderDecoderLSTM(nn.Module):
    """Encoder-Decoder LSTM for sequence-to-sequence forecasting.

    Based on Nearing et al. 2024 (Nature), this architecture separates:
    - Encoder: processes past observations (e.g., 365 days of hindcast)
    - Decoder: processes future forecasts (e.g., 10 days ahead)

    State transfer between encoder and decoder uses:
    - Linear transformation for cell state (preserves long-term memory)
    - Nonlinear (tanh) transformation for hidden state (allows adaptation)

    Parameters
    ----------
    encoder_input_dim : int
        Number of input features for encoder (past observations).
    decoder_input_dim : int
        Number of input features for decoder (future forecasts + uncertainty).
    hidden_dim : int
        Hidden dimension for both encoder and decoder LSTMs.
    head : str
        Type of output head ('GMM', 'CMAL', 'UMAL', or 'Regression').
    head_hidden_dim : int
        Hidden dimension for the output head.
    head_n_dist : int
        Number of mixture components (for GMM, CMAL, UMAL heads).
    dropout : float
        Dropout rate applied to inputs.
    recur_dropout : float
        Recurrent dropout rate applied within LSTM cells.

    References
    ----------
    .. [#] G. Nearing et al.: Global prediction of extreme floods in ungauged watersheds.
           Nature, 2024. https://doi.org/10.1038/s41586-024-07145-1
    """

    def __init__(self, encoder_input_dim: int, decoder_input_dim: int, hidden_dim: int,
                 head: str, head_hidden_dim: int, head_n_dist: int,
                 dropout: float = 0, recur_dropout: float = 0,
                 residual_state_transfer: bool = False):
        super().__init__()

        self.hidden_dim = hidden_dim
        self.encoder_input_dim = encoder_input_dim
        self.decoder_input_dim = decoder_input_dim
        self.residual_state_transfer = residual_state_transfer

        # Encoder LSTM: processes past observations
        self.encoder = LSTM_v1(
            input_dim=encoder_input_dim,
            hidden_dim=hidden_dim,
            adj_matrix=None,
            recur_dropout=recur_dropout,
            dropout=dropout
        )

        # State transfer networks
        # When residual_state_transfer=True: c_dec = c_enc + W @ c_enc (residual connection)
        # When residual_state_transfer=False: c_dec = W @ c_enc + b (standard linear, Nearing et al. 2024)
        # Cell state transfer (preserves long-term memory)
        self.cell_transfer = nn.Linear(hidden_dim, hidden_dim, bias=not residual_state_transfer)
        # Hidden state: nonlinear transfer (allows adaptation to forecast mode)
        # For residual: h_dec = h_enc + tanh(W @ h_enc)
        # For standard: h_dec = tanh(W @ h_enc + b)
        self.hidden_transfer_linear = nn.Linear(hidden_dim, hidden_dim, bias=not residual_state_transfer)

        # Decoder LSTM: processes future forecasts
        self.decoder = LSTM_v1(
            input_dim=decoder_input_dim,
            hidden_dim=hidden_dim,
            adj_matrix=None,
            recur_dropout=recur_dropout,
            dropout=dropout
        )

        # Output head (probabilistic or regression)
        assert head in ['GMM', 'CMAL', 'UMAL', 'Regression'], \
            f"head must be one of ['GMM', 'CMAL', 'UMAL', 'Regression'], got {head}"

        if head == 'GMM':
            self.head_layer = GMM(n_in=hidden_dim,
                                  n_hidden=head_hidden_dim,
                                  n_out=3 * head_n_dist)
        elif head == 'CMAL':
            self.head_layer = CMAL(n_in=hidden_dim,
                                   n_hidden=head_hidden_dim,
                                   n_out=4 * head_n_dist)
        elif head == 'UMAL':
            self.head_layer = UMAL(n_in=hidden_dim,
                                   n_hidden=head_hidden_dim,
                                   n_out=2 * head_n_dist)
        elif head == 'Regression':
            self.head_layer = Regression(n_in=hidden_dim,
                                         n_out=1)

    def _transfer_states(self, h_enc: torch.Tensor, c_enc: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        """Transfer encoder states to decoder initial states.

        Parameters
        ----------
        h_enc : torch.Tensor
            Encoder final hidden state with shape (batch, hidden_dim).
        c_enc : torch.Tensor
            Encoder final cell state with shape (batch, hidden_dim).

        Returns
        -------
        c_dec_init : torch.Tensor
            Decoder initial cell state.
        h_dec_init : torch.Tensor
            Decoder initial hidden state.
        """
        if self.residual_state_transfer:
            # Residual transfer: state = encoder_state + learned_adjustment
            # This makes it easy to learn "pass through unchanged" (W=0)
            # and provides direct gradient path through identity connection
            c_dec_init = c_enc + self.cell_transfer(c_enc)
            h_dec_init = h_enc + torch.tanh(self.hidden_transfer_linear(h_enc))
        else:
            # Standard transfer (Nearing et al. 2024)
            # Cell: linear transformation
            # Hidden: nonlinear transformation
            c_dec_init = self.cell_transfer(c_enc)
            h_dec_init = torch.tanh(self.hidden_transfer_linear(h_enc))

        return c_dec_init, h_dec_init

    def forward(self, x_encoder: torch.Tensor, x_decoder: torch.Tensor,
                encoder_init_states: Tuple[torch.Tensor, torch.Tensor] = None) -> Tuple[Dict[str, torch.Tensor], Tuple[torch.Tensor, torch.Tensor]]:
        """Forward pass through encoder-decoder architecture.

        Parameters
        ----------
        x_encoder : torch.Tensor
            Encoder inputs with shape (batch, encoder_seq_len, encoder_input_dim).
            Contains past observations (met, discharge, lagged chla, static features).
        x_decoder : torch.Tensor
            Decoder inputs with shape (batch, decoder_seq_len, decoder_input_dim).
            Contains future forecasts (met forecasts, PI90 uncertainty, static features).
        encoder_init_states : tuple of torch.Tensor, optional
            Initial (h_0, c_0) states for encoder. If None, initialized to zeros.

        Returns
        -------
        out : Dict[str, torch.Tensor]
            Output from head layer. For CMAL: {'mu', 'b', 'tau', 'pi'} each with
            shape (batch, decoder_seq_len, n_dist).
        final_states : tuple of torch.Tensor
            Final (h_n, c_n) states from decoder, each with shape (batch, hidden_dim).
        """
        # Encode past sequence - we only need final states, not full hidden sequence
        _, (h_enc, c_enc) = self.encoder(x_encoder, encoder_init_states)

        # Transfer states from encoder to decoder
        c_dec_init, h_dec_init = self._transfer_states(h_enc, c_enc)

        # Decode future sequence
        decoder_hidden_seq, (h_final, c_final) = self.decoder(
            x_decoder, (h_dec_init, c_dec_init)
        )

        # Apply output head to each decoder timestep
        out = self.head_layer(decoder_hidden_seq)

        return out, (h_final, c_final)

    def encode_only(self, x_encoder: torch.Tensor,
                    encoder_init_states: Tuple[torch.Tensor, torch.Tensor] = None) -> Tuple[torch.Tensor, torch.Tensor]:
        """Run only the encoder and return transferred states ready for decoder.

        Useful for inference when encoder context is computed once and decoder
        is run multiple times (e.g., for different forecast scenarios).

        Parameters
        ----------
        x_encoder : torch.Tensor
            Encoder inputs with shape (batch, encoder_seq_len, encoder_input_dim).
        encoder_init_states : tuple of torch.Tensor, optional
            Initial (h_0, c_0) states for encoder.

        Returns
        -------
        decoder_init_states : tuple of torch.Tensor
            Transferred (h, c) states ready to initialize decoder.
        """
        _, (h_enc, c_enc) = self.encoder(x_encoder, encoder_init_states)
        c_dec_init, h_dec_init = self._transfer_states(h_enc, c_enc)
        return (h_dec_init, c_dec_init)

    def decode_only(self, x_decoder: torch.Tensor,
                    decoder_init_states: Tuple[torch.Tensor, torch.Tensor]) -> Tuple[Dict[str, torch.Tensor], Tuple[torch.Tensor, torch.Tensor]]:
        """Run only the decoder with pre-computed initial states.

        Parameters
        ----------
        x_decoder : torch.Tensor
            Decoder inputs with shape (batch, decoder_seq_len, decoder_input_dim).
        decoder_init_states : tuple of torch.Tensor
            Initial (h_0, c_0) states for decoder (from encode_only or previous decode).

        Returns
        -------
        out : Dict[str, torch.Tensor]
            Output from head layer.
        final_states : tuple of torch.Tensor
            Final (h_n, c_n) states from decoder.
        """
        decoder_hidden_seq, (h_final, c_final) = self.decoder(
            x_decoder, decoder_init_states
        )
        out = self.head_layer(decoder_hidden_seq)
        return out, (h_final, c_final)

    def forward_autoregressive(self, x_encoder: torch.Tensor, x_decoder: torch.Tensor,
                               chla_lagged_idx: int, chla_unc_idx: int,
                               target_mean: float, target_std: float,
                               decoder_chla_mean: float, decoder_chla_std: float,
                               decoder_unc_mean: float, decoder_unc_std: float,
                               encoder_init_states: Tuple[torch.Tensor, torch.Tensor] = None,
                               use_mean: bool = True) -> Tuple[Dict[str, torch.Tensor], Tuple[torch.Tensor, torch.Tensor]]:
        """Forward pass with autoregressive chla in decoder.

        Instead of using pre-computed lagged chla for all decoder timesteps,
        this method runs the decoder step-by-step, feeding the model's own
        predictions back as lagged input for subsequent timesteps.

        This eliminates the encoder-decoder discontinuity where encoder has
        lagged chla but decoder doesn't, and matches inference behavior.

        Parameters
        ----------
        x_encoder : torch.Tensor
            Encoder inputs with shape (batch, encoder_seq_len, encoder_input_dim).
        x_decoder : torch.Tensor
            Decoder inputs with shape (batch, decoder_seq_len, decoder_input_dim).
            The chla_lagged values at indices 1+ will be replaced with predictions.
            Day 0 chla_lagged should contain the actual t-1 observation.
        chla_lagged_idx : int
            Index of chla_lagged feature in decoder input (last axis).
        chla_unc_idx : int
            Index of chla_uncertainty_lagged feature in decoder input (last axis).
        target_mean : float
            Mean used to scale target variable (for unscaling predictions).
        target_std : float
            Std used to scale target variable (for unscaling predictions).
        decoder_chla_mean : float
            Mean used to scale chla_lagged in decoder inputs.
        decoder_chla_std : float
            Std used to scale chla_lagged in decoder inputs.
        decoder_unc_mean : float
            Mean used to scale chla_uncertainty_lagged in decoder inputs.
        decoder_unc_std : float
            Std used to scale chla_uncertainty_lagged in decoder inputs.
        encoder_init_states : tuple of torch.Tensor, optional
            Initial (h_0, c_0) states for encoder. If None, initialized to zeros.
        use_mean : bool
            Deprecated. The 0.5 quantile (median) is always used for the lagged value,
            as it's a better central estimate for asymmetric distributions than mu.

        Returns
        -------
        out : Dict[str, torch.Tensor]
            Output from head layer. For CMAL: {'mu', 'b', 'tau', 'pi'} each with
            shape (batch, decoder_seq_len, n_dist).
        final_states : tuple of torch.Tensor
            Final (h_n, c_n) states from decoder.
        """
        # Encode past sequence
        _, (h_enc, c_enc) = self.encoder(x_encoder, encoder_init_states)

        # Transfer states from encoder to decoder
        c_dec, h_dec = self._transfer_states(h_enc, c_enc)

        # Get decoder sequence length
        batch_size, seq_len, n_features = x_decoder.shape

        # Clone decoder input so we can modify it
        x_decoder_ar = x_decoder.clone()

        # Storage for outputs
        outputs_mu = []
        outputs_b = []
        outputs_tau = []
        outputs_pi = []

        # Run decoder step-by-step
        h, c = h_dec, c_dec

        for t in range(seq_len):
            # Get decoder input for this timestep
            x_t = x_decoder_ar[:, t:t+1, :]  # (batch, 1, features)

            # If t > 0, replace chla_lagged with previous prediction
            if t > 0:
                # Get previous prediction's distribution parameters (first distribution component)
                prev_mu = outputs_mu[-1][:, 0, 0]  # (batch,) - in scaled target space
                prev_b = outputs_b[-1][:, 0, 0]    # (batch,)
                prev_tau = outputs_tau[-1][:, 0, 0]  # (batch,)

                # Use 0.5 quantile (median) instead of mu for lagged chla
                # Median is often a better central estimate for asymmetric distributions
                prev_median = ald_quantile_torch(0.5, prev_mu, prev_b, prev_tau)

                # Calculate proper PI90 from ALD quantiles: PI90 = Q95 - Q05
                # Result is in scaled target space (width, not centered)
                prev_pi90 = ald_pi90_torch(prev_mu, prev_b, prev_tau)

                # Transform predictions from scaled target space to scaled decoder input space
                # Step 1: Unscale from target space to raw values
                raw_pred = prev_median * target_std + target_mean
                raw_pi90 = prev_pi90 * target_std  # PI90 is a width, only scale by std

                # Step 2: Rescale to decoder input space
                scaled_chla_input = (raw_pred - decoder_chla_mean) / decoder_chla_std
                scaled_unc_input = (raw_pi90 - decoder_unc_mean) / decoder_unc_std

                # Update the decoder input with properly scaled predicted values
                x_t = x_t.clone()
                x_t[:, 0, chla_lagged_idx] = scaled_chla_input
                x_t[:, 0, chla_unc_idx] = scaled_unc_input

            # Run decoder LSTM for one timestep
            decoder_out, (h, c) = self.decoder(x_t, (h, c))

            # Apply head to get distribution parameters
            out_t = self.head_layer(decoder_out)

            # Store outputs
            outputs_mu.append(out_t['mu'])
            outputs_b.append(out_t['b'])
            outputs_tau.append(out_t['tau'])
            outputs_pi.append(out_t['pi'])

        # Stack outputs along sequence dimension
        final_out = {
            'mu': torch.cat(outputs_mu, dim=1),    # (batch, seq_len, n_dist)
            'b': torch.cat(outputs_b, dim=1),      # (batch, seq_len, n_dist)
            'tau': torch.cat(outputs_tau, dim=1),  # (batch, seq_len, n_dist)
            'pi': torch.cat(outputs_pi, dim=1),    # (batch, seq_len, n_dist)
        }

        return final_out, (h, c)
