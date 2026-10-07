import torch
import numpy as np
import time
import pandas as pd
from torch.utils.data import Dataset
from tqdm import tqdm


def rmse_masked(y_true, y_pred):
    """
    Calculate the Root Mean Squared Error (RMSE) between true and predicted values,
    ignoring NaN values in the true values.

    This function computes the RMSE by masking out the NaN values in the `y_true` tensor.
    The RMSE is calculated only on the non-NaN elements of `y_true`.

    Args:
        y_true (torch.Tensor): A tensor containing the true values. NaN values will be ignored.
        y_pred (torch.DataFrame): A DataFrame or similar structure containing the predicted values.
                                  It should have a column named 'y_hat'.

    Returns:
        torch.Tensor: The calculated RMSE value as a tensor.

    Example:
        >>> y_true = torch.tensor([3.0, 5.0, float('nan'), 7.0])
        >>> y_pred = pd.DataFrame({'y_hat': torch.tensor([2.5, 5.5, 6.0, 7.5])})
        >>> rmse_masked(y_true, y_pred)
        tensor(0.6124)
    """
    y_pred = y_pred['y_hat']
    num_y_true = torch.count_nonzero(
        ~torch.isnan(y_true)
    )
    zero_or_error = torch.where(
        torch.isnan(y_true), torch.zeros_like(y_true), y_pred - y_true
    )
    sum_squared_errors = torch.sum(torch.square(zero_or_error))
    rmse_loss = torch.sqrt(sum_squared_errors / num_y_true)
    return rmse_loss


def MaskedGMMLoss(y, prediction, eps = 1e-10):
    """
    Calculate the average negative log-likelihood for a Gaussian Mixture Model (GMM).

    This loss function computes the negative log-likelihood for GMMs, which is commonly used
    in mixture density networks. The implementation handles missing values in the target
    variable `y` by applying a mask, ensuring that only valid (non-NaN) values contribute
    to the loss calculation.

    Parameters
    ----------
    y : torch.Tensor
        The target values (observations) for which the GMM predicts probabilities.
        It should be a tensor of shape (n_samples, n_features).

    prediction : dict
        A dictionary containing the parameters of the GMM:
        - 'mu' (torch.Tensor): The means of the Gaussian components, shape (n_samples, n_components, n_features).
        - 'sigma' (torch.Tensor): The standard deviations of the Gaussian components, shape (n_samples, n_components, n_features).
        - 'pi' (torch.Tensor): The mixture weights (prior probabilities) for each Gaussian component, shape (n_samples, n_components).

    eps : float, optional
        A small constant for numerical stability to prevent log(0). Default is 1e-10.

    Returns
    -------
    torch.Tensor
        The average negative log-likelihood loss across all valid samples.

    References
    ----------
    .. [#] Ha, D. (2015). Mixture density networks with TensorFlow.
           Retrieved from http://blog.otoro.net/2015/11/24/mixture-density-networks-with-tensorflow

    Example
    -------
    >>> y = torch.tensor([[1.0, 2.0], [3.0, 4.0], [float('nan'), 6.0]])
    >>> prediction = {
    ...     'mu': torch.tensor([[[1.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [4.0, 5.0]], [[1.5, 2.5], [3.5, 4.5]]]),
    ...     'sigma': torch.tensor([[[0.5, 0.5], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]]]),
    ...     'pi': torch.tensor([[[0.3, 0.7], [0.6, 0.4]], [[0.4, 0.6], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]]])
    ... }
    >>> loss = MaskedGMMLoss(y, prediction)
    >>> print(loss)
    """

    ONE_OVER_2PI_SQRT = 1.0 / np.sqrt(2.0 * np.pi)

    m = prediction['mu']
    s = prediction['sigma']
    p = prediction['pi']

    mask = ~torch.isnan(y)
    for i in range(y.shape[0]):
        seg_mask = mask[[i]].any(0).any(-1)
        if torch.sum(seg_mask) > 0:

            seg_y = y[[i]][:, seg_mask, :]
            seg_m = m[[i]][:, seg_mask, :]
            seg_s = s[[i]][:, seg_mask, :]
            seg_p = p[[i]][:, seg_mask, :]

            # likelihood calculation
            seg_error = seg_y - seg_m
            seg_result = seg_error * torch.reciprocal(seg_s)
            seg_result = -0.5 * (seg_result * seg_result)
            seg_result = seg_p * ((torch.exp(seg_result) * torch.reciprocal(seg_s)) * ONE_OVER_2PI_SQRT)

            # concatenate all the likelihoods
            try:
                total_result = torch.cat([total_result, seg_result], dim = 1)
            except NameError as e:
                total_result = seg_result

    # Sum across n distribution
    result = torch.sum(total_result, dim=-1)
    # Take the negative log
    result = -torch.log(result + eps)
    # Take the average
    result = torch.sum(result) / torch.sum(mask)
    return(result)


def MaskedGMMLoss_weighted(y, prediction, weights, eps = 1e-10):
    """
    Calculate the average negative log-likelihood for a Gaussian Mixture Model (GMM) with weighted contributions.

    This loss function computes the negative log-likelihood for GMMs, which is commonly used
    in mixture density networks. The implementation handles missing values in the target
    variable `y` by applying a mask, ensuring that only valid (non-NaN) values contribute
    to the loss calculation. Additionally, it incorporates weights for each sample, allowing
    for flexible loss contributions based on the importance of each observation.

    Parameters
    ----------
    y : torch.Tensor
        The target values (observations) for which the GMM predicts probabilities.
        It should be a tensor of shape (n_samples, n_features).

    prediction : dict
        A dictionary containing the parameters of the GMM:
        - 'mu' (torch.Tensor): The means of the Gaussian components, shape (n_samples, n_components, n_features).
        - 'sigma' (torch.Tensor): The standard deviations of the Gaussian components, shape (n_samples, n_components, n_features).
        - 'pi' (torch.Tensor): The mixture weights (prior probabilities) for each Gaussian component, shape (n_samples, n_components).

    weights : torch.Tensor
        A tensor of shape (n_samples, n_components, n_features) representing the weights for each sample.
        These weights determine the contribution of each sample to the overall loss.

    eps : float, optional
        A small constant for numerical stability to prevent log(0). Default is 1e-10.

    Returns
    -------
    torch.Tensor
        The average negative log-likelihood loss across all valid samples,
        weighted by the provided weights.

    References
    ----------
    .. [#] Ha, D. (2015). Mixture density networks with TensorFlow.
           Retrieved from http://blog.otoro.net/2015/11/24/mixture-density-networks-with-tensorflow

    Example
    -------
    >>> y = torch.tensor([[1.0, 2.0], [3.0, 4.0], [float('nan'), 6.0]])
    >>> prediction = {
    ...     'mu': torch.tensor([[[1.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [4.0, 5.0]], [[1.5, 2.5], [3.5, 4.5]]]),
    ...     'sigma': torch.tensor([[[0.5, 0.5], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]]]),
    ...     'pi': torch.tensor([[[0.3, 0.7], [0.6, 0.4]], [[0.4, 0.6], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]]])
    ... }
    >>> weights = torch.tensor([[[1.0], [1.0]], [[1.0], [1.0]], [[1.0], [1.0]]])  # Example weights
    >>> loss = MaskedGMMLoss_weighted(y, prediction, weights)
    >>> print(loss)
    """

    ONE_OVER_2PI_SQRT = 1.0 / np.sqrt(2.0 * np.pi)

    m = prediction['mu']
    s = prediction['sigma']
    p = prediction['pi']

    mask = ~torch.isnan(y)
    for i in range(y.shape[0]):
        seg_mask = mask[[i]].any(0).any(-1)
        if torch.sum(seg_mask) > 0:

            seg_y = y[[i]][:, seg_mask, :]
            seg_m = m[[i]][:, seg_mask, :]
            seg_s = s[[i]][:, seg_mask, :]
            seg_p = p[[i]][:, seg_mask, :]

            # likelihood calculation
            seg_error = seg_y - seg_m
            seg_result = seg_error * torch.reciprocal(seg_s)
            seg_result = -0.5 * (seg_result * seg_result)
            seg_result = seg_p * ((torch.exp(seg_result) * torch.reciprocal(seg_s)) * ONE_OVER_2PI_SQRT)

            # concatenate all the likelihoods
            try:
                total_result = torch.cat([total_result, seg_result], dim = 1)
                all_weights = torch.cat([all_weights, weights[[i]][:, seg_mask, :]], dim = 1)
            except NameError as e:
                total_result = seg_result
                all_weights = weights[[i]][:, seg_mask, :]

    # Sum the likelihoods across all Gaussian distributions for each sample
    result = torch.sum(total_result, dim=-1)
    # Compute the negative log-likelihood; add a small constant (eps) for numerical stability
    result = -torch.log(result + eps)
    # Apply the input weights to the result, scaling the negative log-likelihood accordingly
    result = all_weights[:,:,0] * result
    # Calculate the average loss by dividing the total weighted loss by the count of valid samples
    result = torch.sum(result) / torch.sum(mask)
    return(result)


def MaskedCMALLoss(y, prediction, eps = 1e-8):
    """
    Calculate the average negative log-likelihood for a model using the CMAL (Countable Mixtures of Asymmetric Laplacians) head.

    This loss function computes the average negative log-likelihood based on the CMAL framework,
    which is designed to model complex distributions. The implementation handles missing values
    in the target variable `y` by applying a mask, ensuring that only valid (non-NaN) values contribute
    to the loss calculation.

    Parameters
    ----------
    y : torch.Tensor
        The target values (observations) for which the CMAL model predicts probabilities.
        It should be a tensor of shape (n_samples, n_features).

    prediction : dict
        A dictionary containing the parameters of the CMAL model:
        - 'mu' (torch.Tensor): The predicted means, shape (n_samples, n_components, n_features).
        - 'b' (torch.Tensor): The predicted scale parameters, shape (n_samples, n_components, n_features).
        - 'tau' (torch.Tensor): The predicted transformation parameters, shape (n_samples, n_components, n_features).
        - 'pi' (torch.Tensor): The mixture weights (prior probabilities) for each component, shape (n_samples, n_components).

    eps : float, optional
        A small constant for numerical stability to prevent log(0). Default is 1e-8.

    Returns
    -------
    torch.Tensor
        The average negative log-likelihood loss across all valid samples.

    Example
    -------
    >>> y = torch.tensor([[1.0, 2.0], [3.0, float('nan'), 4.0]])
    >>> prediction = {
    ...     'mu': torch.tensor([[[1.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [4.0, 5.0]]]),
    ...     'b': torch.tensor([[[0.5, 0.5], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]]]),
    ...     'tau': torch.tensor([[[0.1, 0.1], [0.1, 0.1]], [[0.2, 0.2], [0.2, 0.2]]]),
    ...     'pi': torch.tensor([[[0.3, 0.7], [0.6, 0.4]], [[0.4, 0.6], [0.5, 0.5]]])
    ... }
    >>> loss = MaskedCMALLoss(y, prediction)
    >>> print(loss)
    """

    m = prediction['mu']
    b = prediction['b']
    t = prediction['tau']
    p = prediction['pi']

    # Initialize as None to track if any valid data was found
    total_log_like = None
    total_log_weights = None

    mask = ~torch.isnan(y)
    for i in range(y.shape[0]):
        seg_mask = mask[[i]].any(0).any(-1)
        if torch.sum(seg_mask) > 0:

            seg_y = y[[i]][:, seg_mask, :]
            seg_m = m[[i]][:, seg_mask, :]
            seg_b = b[[i]][:, seg_mask, :]
            seg_t = t[[i]][:, seg_mask, :]
            seg_p = p[[i]][:, seg_mask, :]

            # likelihood calculation
            seg_error = seg_y - seg_m
            seg_log_like = torch.log(seg_t) + \
               torch.log(1.0 - seg_t) - \
               torch.log(seg_b) - \
               torch.max(seg_t * seg_error, (seg_t - 1.0) * seg_error) / seg_b
            seg_log_weights = torch.log(seg_p + eps)

            # concatenate all the likelihoods
            if total_log_like is None:
                total_log_like = seg_log_like
                total_log_weights = seg_log_weights
            else:
                total_log_like = torch.cat([total_log_like , seg_log_like], dim = 1)
                total_log_weights = torch.cat([total_log_weights, seg_log_weights], dim = 1)

    # Handle case where no valid observations exist
    if total_log_like is None:
        return torch.tensor(0.0, device=y.device, requires_grad=True)

    # Aggregate
    result = torch.logsumexp(total_log_weights + total_log_like, dim=2)
    result = -torch.mean(torch.sum(result, dim=1))
    return(result)


def MaskedCMALLoss_with_obs_uncertainty(y_obs, obs_pi90, prediction, n_mc_samples=50, eps=1e-8):
    """
    CMAL loss with observation uncertainty via Monte Carlo integration.

    This loss function accounts for observation uncertainty by convolving the
    predicted Asymmetric Laplace Distribution (ALD) with a Gaussian observation
    error model. The marginal likelihood is estimated using Monte Carlo sampling.

    The mathematical framework:
        - Observation model: y_obs = y_true + ε, where ε ~ N(0, σ_obs²)
        - Model predicts: p(y_true | x) = CMAL mixture of ALDs
        - Marginal likelihood: p(y_obs | x, σ_obs) = ∫ N(y_obs; y_true, σ_obs²) · ALD(y_true) dy_true

    Uses existing `_sample_asymmetric_laplacians` from sampling_utils for correct
    ALD sampling with numerical stability.

    Parameters
    ----------
    y_obs : torch.Tensor
        Observed values (may include NaN), shape (batch, seq, 1) or (batch, seq, n_targets)
    obs_pi90 : torch.Tensor
        Observation uncertainty as PI90 (90% prediction interval width),
        shape (batch, seq, 1) or (batch, seq, n_targets).
        Internally converted to SD: σ = PI90 / 3.29
    prediction : dict
        Model predictions containing:
        - 'mu': Location parameters, shape (batch, seq, n_dist)
        - 'b': Scale parameters, shape (batch, seq, n_dist)
        - 'tau': Asymmetry parameters, shape (batch, seq, n_dist)
        - 'pi': Mixture weights, shape (batch, seq, n_dist)
    n_mc_samples : int
        Number of Monte Carlo samples for integration (default 50)
    eps : float
        Small constant for numerical stability (default 1e-8)

    Returns
    -------
    loss : torch.Tensor
        Scalar negative log-likelihood loss

    Notes
    -----
    - High obs_pi90 values → large σ_obs → Gaussian likelihood is "wider" →
      model predictions contribute less to loss (appropriately down-weighted)
    - Low obs_pi90 values → small σ_obs → Gaussian likelihood is "tighter" →
      model must predict close to observation

    The Monte Carlo estimate:
        p(y_obs | x, σ) ≈ (1/N) Σᵢ N(y_obs; y_sample_i, σ²)
    where y_sample_i ~ ALD(μ, b, τ) are samples from the predicted distribution.
    """
    # Import here to avoid circular import
    from src.sampling_utils import _sample_asymmetric_laplacians

    m = prediction['mu']       # (batch, seq, n_dist)
    b = prediction['b']        # (batch, seq, n_dist)
    t = prediction['tau']      # (batch, seq, n_dist)
    p = prediction['pi']       # (batch, seq, n_dist)

    # Convert PI90 to standard deviation
    # For Gaussian: PI90 = Q95 - Q05 ≈ 3.29 × σ
    sigma_obs = obs_pi90 / 3.29

    # Create mask for valid observations
    mask = ~torch.isnan(y_obs)

    # Process each batch segment (following pattern from MaskedCMALLoss)
    for i in range(y_obs.shape[0]):
        seg_mask = mask[[i]].any(0).any(-1)
        if torch.sum(seg_mask) > 0:
            # Extract valid segments
            seg_y = y_obs[[i]][:, seg_mask, :]           # (1, n_valid, n_targets)
            seg_sigma = sigma_obs[[i]][:, seg_mask, :]   # (1, n_valid, n_targets)
            seg_m = m[[i]][:, seg_mask, :]               # (1, n_valid, n_dist)
            seg_b = b[[i]][:, seg_mask, :]               # (1, n_valid, n_dist)
            seg_t = t[[i]][:, seg_mask, :]               # (1, n_valid, n_dist)
            seg_p = p[[i]][:, seg_mask, :]               # (1, n_valid, n_dist)

            n_valid = seg_y.shape[1]
            n_dist = seg_m.shape[2]

            # For each mixture component, compute marginal log-likelihood via MC
            component_log_likes = []

            for k in range(n_dist):
                # Repeat parameters for MC sampling
                # Shape: (1, n_valid, 1) -> (1, n_valid, n_mc_samples)
                m_k = seg_m[:, :, k:k+1].repeat(1, 1, n_mc_samples)
                b_k = seg_b[:, :, k:k+1].repeat(1, 1, n_mc_samples)
                t_k = seg_t[:, :, k:k+1].repeat(1, 1, n_mc_samples)

                # Reshape for _sample_asymmetric_laplacians
                # Function expects (n_samples, ...) format
                m_flat = m_k.squeeze(0)  # (n_valid, n_mc_samples)
                b_flat = b_k.squeeze(0)
                t_flat = t_k.squeeze(0)

                # Sample from ALD: y_samples ~ ALD(μ, b, τ)
                y_samples = _sample_asymmetric_laplacians(m_flat, b_flat, t_flat)
                y_samples = y_samples.unsqueeze(0)  # Back to (1, n_valid, n_mc_samples)

                # Expand observation and sigma for broadcasting
                # y_obs: (1, n_valid, n_targets) -> need (1, n_valid, n_mc_samples)
                y_expanded = seg_y.expand(-1, -1, n_mc_samples)
                sigma_expanded = seg_sigma.expand(-1, -1, n_mc_samples)

                # Gaussian log-likelihood: log N(y_obs | y_sample, σ²)
                # log N(y|μ,σ²) = -0.5 * ((y-μ)/σ)² - log(σ) - 0.5*log(2π)
                gaussian_ll = (
                    -0.5 * ((y_expanded - y_samples) / (sigma_expanded + eps)) ** 2
                    - torch.log(sigma_expanded + eps)
                    - 0.5 * np.log(2 * np.pi)
                )

                # Monte Carlo estimate: log( (1/N) Σ exp(gaussian_ll) )
                # = logsumexp(gaussian_ll, dim=-1) - log(N)
                marginal_ll_k = torch.logsumexp(gaussian_ll, dim=-1, keepdim=True) - np.log(n_mc_samples)
                # Shape: (1, n_valid, 1)

                component_log_likes.append(marginal_ll_k)

            # Stack components: (1, n_valid, n_dist)
            component_log_likes = torch.cat(component_log_likes, dim=-1)

            # Mixture weighting: log p(y_obs) = logsumexp(log(π) + log_marginal_ll)
            seg_log_weights = torch.log(seg_p + eps)
            seg_mixture_ll = torch.logsumexp(seg_log_weights + component_log_likes, dim=-1)
            # Shape: (1, n_valid)

            # Concatenate across batches
            try:
                total_mixture_ll = torch.cat([total_mixture_ll, seg_mixture_ll], dim=1)
            except NameError:
                total_mixture_ll = seg_mixture_ll

    # Return negative mean log-likelihood
    result = -torch.mean(total_mixture_ll)
    return result


def MaskedUMALLoss(y, taus, n_taus, prediction, eps = 1e-5):
    """
    Calculate the average negative log-likelihood for a model using the UMAL (Uncountable Mixtures of Asymmetric Laplacians) head.

    This loss function computes the average negative log-likelihood based on the UMAL framework,
    which is designed to model complex distributions effectively. The implementation handles missing
    values in the target variable `y` by applying a mask, ensuring that only valid (non-NaN) values
    contribute to the loss calculation.

    Parameters
    ----------
    y : torch.Tensor
        The target values (observations) for which the UMAL model predicts probabilities.
        It should be a tensor of shape (n_samples, n_features).

    taus : torch.Tensor
        A tensor of shape (n_samples, n_taus, n_features) representing the transformation parameters
        for the model.

    n_taus : int
        The number of transformation parameters (taus) used in the model.

    prediction : dict
        A dictionary containing the parameters of the UMAL model:
        - 'mu' (torch.Tensor): The predicted means, shape (n_samples, n_components, n_features).
        - 'b' (torch.Tensor): The predicted scale parameters, shape (n_samples, n_components, n_features).

    eps : float, optional
        A small constant for numerical stability to prevent log(0). Default is 1e-5.

    Returns
    -------
    torch.Tensor
        The average negative log-likelihood loss across all valid samples.

    Example
    -------
    >>> y = torch.tensor([[1.0, 2.0], [3.0, float('nan'), 4.0]])
    >>> taus = torch.tensor([[[0.1, 0.2], [0.3, 0.4]], [[0.2, 0.3], [0.4, 0.5]]])
    >>> n_taus = 2
    >>> prediction = {
    ...     'mu': torch.tensor([[[1.0, 2.0], [3.0, 4.0]], [[2.0, 3.0], [4.0, 5.0]]]),
    ...     'b': torch.tensor([[[0.5, 0.5], [0.5, 0.5]], [[0.5, 0.5], [0.5, 0.5]]])
    ... }
    >>> loss = MaskedUMALLoss(y, taus, n_taus, prediction)
    >>> print(loss)
    """

    t = taus
    m = prediction['mu']
    b = prediction['b']

    mask = ~torch.isnan(y)
    for i in range(y.shape[0]):
        seg_mask = mask[[i]].any(0).any(-1)
        if torch.sum(seg_mask) > 0:

            seg_y = y[[i]][:, seg_mask, :]
            seg_m = m[[i]][:, seg_mask, :]
            seg_b = b[[i]][:, seg_mask, :]
            seg_t = t[[i]][:, seg_mask, :]

            # likelihood calculation
            seg_error = seg_y - seg_m
            seg_log_like = torch.log(seg_t) + \
               torch.log(1.0 - seg_t) - \
               torch.log(seg_b) - \
               torch.max(seg_t * seg_error, (seg_t - 1.0) * seg_error) / seg_b

            n_taus_log = torch.as_tensor(np.log(n_taus).astype('float32'))

            original_batch_size = int(seg_log_like.shape[0] / n_taus)
            seg_log_like_split = torch.cat(seg_log_like[:, :, :].split(original_batch_size, 0), 2)


            # concatenate all the likelihoods
            try:
                total_log_like = torch.cat([total_log_like , seg_log_like_split], dim = 1)
            except NameError as e:
                total_log_like = seg_log_like_split

    # Aggregate
    result = torch.logsumexp(total_log_like, dim=2) - n_taus_log
    result = -torch.mean(torch.sum(result, dim=1))
    return(result)

def get_UMAL_taus(data, n_taus, tau_min, tau_max, batch_size, extend_batch):
    """
    Generate random tau values for the UMAL (Uncountable Mixtures of Asymmetric Laplacians) model.

    This function creates a tensor of tau values sampled uniformly from a specified range
    defined by `tau_min` and `tau_max`. The generated tau values can either be created
    for a standard batch size or extended to cover multiple taus per batch.

    Parameters
    ----------
    data : torch.Tensor
        The input data tensor, shape (seq_length, n_features). The sequence length is used
        to determine how many times the tau values should be repeated.

    n_taus : int
        The number of tau values to generate for each batch.

    tau_min : float
        The minimum value for the tau sampling range.

    tau_max : float
        The maximum value for the tau sampling range.

    batch_size : int
        The number of samples in each batch.

    extend_batch : bool
        If True, generates tau values for each of the `n_taus` for the given batch size.
        If False, generates a single tau value per sample in the batch.

    Returns
    -------
    torch.Tensor
        A tensor of shape (seq_length, batch_size * n_taus, 1) if `extend_batch` is True,
        or (seq_length, batch_size, 1) if `extend_batch` is False, containing the generated tau values.
        
    Example
    -------
    >>> data = torch.randn(5, 3)  # Example data with seq_length=5 and n_features=3
    >>> n_taus = 2
    >>> tau_min = 0.1
    >>> tau_max = 1.0
    >>> batch_size = 4
    >>> extend_batch = True
    >>> taus = get_UMAL_taus(data, n_taus, tau_min, tau_max, batch_size, extend_batch)
    >>> print(taus.shape)  # Should print: torch.Size([5, 8, 1]) if extend_batch is True
    """
    seq_length = data.shape[0]
    if extend_batch:
        taus = ((tau_max - tau_min) * torch.rand(1, batch_size * n_taus, 1) + tau_min)
    else:
        taus = ((tau_max - tau_min) * torch.rand(1, batch_size, 1) + tau_min)
    taus = taus.repeat(seq_length, 1, 1)
    return(taus)

def fit_torch_model(model, x, y, h, c, weighting_matrix,
                    epochs, loss_fn, optimizer, gpu, head, early_stopping_patience, weights_file,
                    umal_extend_batch, umal_n_taus_train, umal_tau_min, umal_tau_max,
                    weight_loss, weight_threshold, weight_value):
    """
    Train a PyTorch model using the specified parameters and data.

    This function fits a PyTorch model to the provided training data over a specified number
    of epochs. It supports the option to utilize GPU for training if available and requested.
    The function also accommodates different loss functions based on the specified head.

    Parameters
    ----------
    model : torch.nn.Module
        The PyTorch model to be trained.

    x : torch.Tensor
        The input data tensor, shape (n_samples, n_features).

    y : torch.Tensor
        The target values tensor, shape (n_samples, n_targets).

    h : torch.Tensor
        The initial hidden state tensor for the LSTM, shape (n_layers, batch_size, hidden_size).

    c : torch.Tensor
        The initial cell state tensor for the LSTM, shape (n_layers, batch_size, hidden_size).

    weighting_matrix : torch.Tensor
        A matrix used for weighting the loss function, shape (n_samples, n_targets).

    epochs : int
        The number of epochs to train the model.

    loss_fn : callable
        The loss function to be used for training. It should accept the target and predicted values.

    optimizer : torch.optim.Optimizer
        The optimizer used for updating model weights.

    gpu : bool
        If True, the model and data will be moved to GPU for training if available.

    head : str
        The type of model head to use.

    umal_extend_batch : bool
        If True, generates multiple tau values for each sample in the batch.

    umal_n_taus_train : int
        The number of tau values to generate during training for the UMAL head.

    umal_tau_min : float
        The minimum value for tau sampling.

    umal_tau_max : float
        The maximum value for tau sampling.

    weight_loss : bool
        If True, applies weighting to the loss based on the specified conditions.

    weight_threshold : float
        The temperature threshold above which observations will be assigned a higher weight in the loss function.

    weight_value : float
        The factor by which to weight observations that exceed the threshold. For example, a value of 2 means these observations will contribute twice as much to the loss as those below the threshold.

    Returns
    -------
    torch.nn.Module
        The trained PyTorch model.

    Example
    -------
    >>> model = MyModel()  # Replace with your model class
    >>> x = torch.randn(100, 10)  # Example input data
    >>> y = torch.randn(100, 1)    # Example target data
    >>> h = torch.zeros((1, 100, 64))  # Initial hidden state
    >>> c = torch.zeros((1, 100, 64))  # Initial cell state
    >>> weighting_matrix = torch.ones(100, 1)  # Weighting matrix
    >>> epochs = 10
    >>> loss_fn = torch.nn.MSELoss()  # Example loss function
    >>> optimizer = torch.optim.Adam(model.parameters())  # Example optimizer
    >>> gpu = True  # Use GPU if available
    >>> head = 'UMAL'
    >>> umal_extend_batch = True
    >>> umal_n_taus_train = 5
    >>> umal_tau_min = 0.1
    >>> umal_tau_max = 1.0
    >>> trained_model = fit_torch_model(model, x, y, h, c, weighting_matrix,
    ...                                   epochs, loss_fn, optimizer, gpu, head,
    ...                                   umal_extend_batch, umal_n_taus_train,
    ...                                   umal_tau_min, umal_tau_max,
    ...                                   weight_loss, weight_threshold, weight_value)
    """
    # moving to gpu if available and requested
    # specifying request because gpu can be slower for small models
    if gpu == True:
        device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
        model.to(device)
        x = x.to(device)
        y = y.to(device)
        h = h.to(device)
        c = c.to(device)
        weighting_matrix = weighting_matrix.to(device)
    else:
        device = torch.device('cpu')
        model.to(device)
        x = x.to(device)
        y = y.to(device)
        h = h.to(device)
        c = c.to(device)
        weighting_matrix = weighting_matrix.to(device)

    if not early_stopping_patience:
        early_stopping_patience = epochs

    epochs_since_best = 0
    best_loss = 1000000000 # Will get overwritten

    # Initialize weights for loss calculation
    weights = torch.ones(y.shape)
    if weight_loss:
        # Assign higher weights to observations exceeding the threshold
        weights[y > weight_threshold] = weight_value

    for i in range(epochs):
        start_time = time.time()

        out, (h, c) = model(x, (h.detach(), c.detach()), weighting_matrix) # stateful lstm
            # .detach() because prev h/c are tied to gradients/weights of
            # a different iteration

        if head == 'UMAL':
            taus = get_UMAL_taus(y, umal_n_taus_train, umal_tau_min, umal_tau_min, x.shape[0], umal_extend_batch)
            loss = loss_fn(y, taus, umal_n_taus_train, out)
        else:
            if weight_loss:
                loss = loss_fn(y, out, weights)
            else:
                loss = loss_fn(y, out)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        end_time = time.time()
        loop_time = end_time - start_time

        print('Epoch %i/' %(i+1) + str(epochs), flush = True)
        print('[==============================]',
              '{0:.2f}'.format(loop_time) + 's/step',
              '- loss: ' + '{0:.4f}'.format(loss.item()),
              flush = True)

        if loss < best_loss:
            torch.save(model.state_dict(), weights_file)
            best_loss = loss
            epochs_since_best = 0
        else:
            epochs_since_best += 1
        if epochs_since_best > early_stopping_patience:
            print(f"Early Stopping at Epoch {i}")
            break

    # move back to cpu when complete for simplicity
    model.to('cpu')
    return(model)

def unscale_output(y_scl, y_std, y_mean):
    """
    unscale output data given a standard deviation and a mean value for the
    outputs
    :param y_scl: [numpy array] scaled output data (predicted or observed)
    :param y_std:[numpy array] array of standard deviation of variables_to_log [n_out]
    :param y_mean:[numpy array] array of variable means [n_out]
    :return: unscaled data
    """
    y_unscaled = y_scl.copy()

    y_unscaled = (y_scl * (y_std + 1e-10)) + y_mean

    return y_unscaled


## Generic PyTorch Training Routine
def train_loop(epoch_index,
               dataloader,
               h,
               c,
               weighting_matrix,
               head,
               model,
               loss_function,
               optimizer,
               umal_extend_batch,
               umal_n_taus_train,
               umal_tau_min,
               umal_tau_max,
               weight_loss,
               weight_threshold,
               weight_value,
               device = 'cpu'):
    """
    @param epoch_index: [int] Epoch number
    @param dataloader: [object] torch dataloader with train and val data
    @param model: [object] initialized torch model
    @param loss_function: loss function
    @param optimizer: [object] Chosen optimizer
    @param device: [str] cpu or gpu
    @return: [float] epoch loss
    """
    train_loss=[]
    with tqdm(dataloader, ncols=100, desc= f"Epoch {epoch_index+1}", unit="batch") as tepoch:
        for x, y in tepoch:
            trainx = x.to(device)
            trainy = y.to(device)

            # Initialize weights for loss calculation
            weights = torch.ones(trainy.shape)
            if weight_loss:
                # Assign higher weights to observations exceeding the threshold
                weights[trainy > weight_threshold] = weight_value

            optimizer.zero_grad()
            output, (h, c) = model(trainx, (h.detach(), c.detach()), weighting_matrix)
            if head == 'UMAL':
                taus = get_UMAL_taus(trainy, umal_n_taus_train, umal_tau_min, umal_tau_min, trainx.shape[0], umal_extend_batch)
                loss = loss_function(trainy, taus, umal_n_taus_train, output)
            else:
                if weight_loss:
                    loss = loss_function(trainy, output, weights)
                else:
                    loss = loss_function(trainy, output)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 3)
            optimizer.step()
            train_loss.append(loss.item())
            tepoch.set_postfix(loss=loss.item())
    mean_loss = np.mean(train_loss)
    return mean_loss

def val_loop(dataloader,
             h,
             c,
             weighting_matrix,
             head,
             model,
             loss_function,
             umal_extend_batch,
             umal_n_taus_train,
             umal_tau_min,
             umal_tau_max,
             weight_loss,
             weight_threshold,
             weight_value,
             device = 'cpu'):
    """
    @param dataloader: [object] torch dataloader with train and val data
    @param model: [object] initialized torch model
    @param loss_function: loss function
    @param device: [str] cpu or gpu
    @return: [float] epoch validation loss
    """
    val_loss = []
    for iter, (x, y) in enumerate(dataloader):
        testx = x.to(device)
        testy = y.to(device)
        # Initialize weights for loss calculation
        weights = torch.ones(testy.shape)
        if weight_loss:
            # Assign higher weights to observations exceeding the threshold
            weights[testy > weight_threshold] = weight_value

        output, (h, c) = model(testx, (h.detach(), c.detach()), weighting_matrix)
        if head == 'UMAL':
            taus = get_UMAL_taus(testy, umal_n_taus_train, umal_tau_min, umal_tau_min, testx.shape[0], umal_extend_batch)
            loss = loss_function(testy, taus, umal_n_taus_train, output)
        else:
            if weight_loss:
                loss = loss_function(testy, output, weights)
            else:
                loss = loss_function(testy, output)
        val_loss.append(loss.item())
    mval_loss = np.mean(val_loss)
    print(f"Valid loss: {mval_loss:.2f}")
    return mval_loss

## Encoder-Decoder Training Loops
def train_loop_encoder_decoder(epoch_index,
                               dataloader,
                               head,
                               model,
                               loss_function,
                               optimizer,
                               device='cpu',
                               use_obs_uncertainty=False,
                               n_mc_samples=50,
                               autoregressive=False,
                               chla_lagged_idx=None,
                               chla_unc_idx=None,
                               ar_scaling_params=None):
    """
    Training loop for encoder-decoder LSTM architecture.

    @param epoch_index: [int] Epoch number
    @param dataloader: [object] torch dataloader yielding (x_encoder, x_decoder, y) tuples,
                       or (x_encoder, x_decoder, y, y_obs_pi90) if use_obs_uncertainty=True
    @param head: [str] Type of output head (CMAL, GMM, etc.)
    @param model: [object] EncoderDecoderLSTM model
    @param loss_function: loss function (used when use_obs_uncertainty=False)
    @param optimizer: [object] Chosen optimizer
    @param device: [str] cpu or gpu
    @param use_obs_uncertainty: [bool] Whether to use observation uncertainty in loss
    @param n_mc_samples: [int] Number of MC samples if using observation uncertainty
    @param autoregressive: [bool] Whether to use autoregressive decoder mode
    @param chla_lagged_idx: [int] Index of chla_lagged in decoder features (required if autoregressive=True)
    @param chla_unc_idx: [int] Index of chla_uncertainty_lagged in decoder features (required if autoregressive=True)
    @param ar_scaling_params: [dict] Scaling params for autoregressive mode with keys:
                              target_mean, target_std, decoder_chla_mean, decoder_chla_std,
                              decoder_unc_mean, decoder_unc_std
    @return: [float] epoch loss
    """
    if autoregressive and (chla_lagged_idx is None or chla_unc_idx is None):
        raise ValueError("chla_lagged_idx and chla_unc_idx must be provided when autoregressive=True")
    if autoregressive and ar_scaling_params is None:
        raise ValueError("ar_scaling_params must be provided when autoregressive=True")

    train_loss = []
    with tqdm(dataloader, ncols=100, desc=f"Epoch {epoch_index+1}", unit="batch") as tepoch:
        for batch in tepoch:
            if use_obs_uncertainty:
                x_enc, x_dec, y, y_obs_pi90 = batch
                y_obs_pi90 = y_obs_pi90.to(device)
            else:
                x_enc, x_dec, y = batch

            x_enc = x_enc.to(device)
            x_dec = x_dec.to(device)
            y = y.to(device)

            optimizer.zero_grad()

            # Forward pass through encoder-decoder
            if autoregressive:
                # Autoregressive mode: decoder uses its own predictions as lagged input
                output, (h_final, c_final) = model.forward_autoregressive(
                    x_enc, x_dec,
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
                # Standard mode: decoder uses pre-computed lagged values
                output, (h_final, c_final) = model(x_enc, x_dec)

            # Compute loss over decoder outputs
            if use_obs_uncertainty and head == 'CMAL':
                loss = MaskedCMALLoss_with_obs_uncertainty(
                    y, y_obs_pi90, output, n_mc_samples=n_mc_samples
                )
            else:
                loss = loss_function(y, output)

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 3)
            optimizer.step()

            train_loss.append(loss.item())
            tepoch.set_postfix(loss=loss.item())

    mean_loss = np.mean(train_loss)
    return mean_loss


def val_loop_encoder_decoder(dataloader,
                             head,
                             model,
                             loss_function,
                             device='cpu',
                             use_obs_uncertainty=False,
                             n_mc_samples=50,
                             autoregressive=False,
                             chla_lagged_idx=None,
                             chla_unc_idx=None,
                             ar_scaling_params=None):
    """
    Validation loop for encoder-decoder LSTM architecture.

    @param dataloader: [object] torch dataloader yielding (x_encoder, x_decoder, y) tuples,
                       or (x_encoder, x_decoder, y, y_obs_pi90) if use_obs_uncertainty=True
    @param head: [str] Type of output head (CMAL, GMM, etc.)
    @param model: [object] EncoderDecoderLSTM model
    @param loss_function: loss function (used when use_obs_uncertainty=False)
    @param device: [str] cpu or gpu
    @param use_obs_uncertainty: [bool] Whether to use observation uncertainty in loss
    @param n_mc_samples: [int] Number of MC samples if using observation uncertainty
    @param autoregressive: [bool] Whether to use autoregressive decoder mode
    @param chla_lagged_idx: [int] Index of chla_lagged in decoder features (required if autoregressive=True)
    @param chla_unc_idx: [int] Index of chla_uncertainty_lagged in decoder features (required if autoregressive=True)
    @param ar_scaling_params: [dict] Scaling params for autoregressive mode with keys:
                              target_mean, target_std, decoder_chla_mean, decoder_chla_std,
                              decoder_unc_mean, decoder_unc_std
    @return: [float] epoch validation loss
    """
    if autoregressive and (chla_lagged_idx is None or chla_unc_idx is None):
        raise ValueError("chla_lagged_idx and chla_unc_idx must be provided when autoregressive=True")
    if autoregressive and ar_scaling_params is None:
        raise ValueError("ar_scaling_params must be provided when autoregressive=True")

    val_loss = []
    with torch.no_grad():
        for batch in dataloader:
            if use_obs_uncertainty:
                x_enc, x_dec, y, y_obs_pi90 = batch
                y_obs_pi90 = y_obs_pi90.to(device)
            else:
                x_enc, x_dec, y = batch

            x_enc = x_enc.to(device)
            x_dec = x_dec.to(device)
            y = y.to(device)

            # Forward pass through encoder-decoder
            if autoregressive:
                output, (h_final, c_final) = model.forward_autoregressive(
                    x_enc, x_dec,
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
                output, (h_final, c_final) = model(x_enc, x_dec)

            # Compute loss over decoder outputs
            if use_obs_uncertainty and head == 'CMAL':
                loss = MaskedCMALLoss_with_obs_uncertainty(
                    y, y_obs_pi90, output, n_mc_samples=n_mc_samples
                )
            else:
                loss = loss_function(y, output)

            val_loss.append(loss.item())

    mval_loss = np.mean(val_loss)
    print(f"Valid loss: {mval_loss:.2f}")
    return mval_loss


def val_loop_encoder_decoder_by_horizon(dataloader,
                                        head,
                                        model,
                                        loss_function,
                                        decoder_seq_len,
                                        device='cpu',
                                        use_obs_uncertainty=False,
                                        n_mc_samples=50,
                                        autoregressive=False,
                                        chla_lagged_idx=None,
                                        chla_unc_idx=None,
                                        ar_scaling_params=None):
    """
    Validation loop for encoder-decoder that tracks metrics per forecast horizon.

    @param dataloader: [object] torch dataloader yielding (x_encoder, x_decoder, y) tuples,
                       or (x_encoder, x_decoder, y, y_obs_pi90) if use_obs_uncertainty=True
    @param head: [str] Type of output head (CMAL, GMM, etc.)
    @param model: [object] EncoderDecoderLSTM model
    @param loss_function: loss function (used when use_obs_uncertainty=False)
    @param decoder_seq_len: [int] Length of decoder sequence (forecast horizon)
    @param device: [str] cpu or gpu
    @param use_obs_uncertainty: [bool] Whether to use observation uncertainty in loss
    @param n_mc_samples: [int] Number of MC samples if using observation uncertainty
    @param autoregressive: [bool] Whether to use autoregressive decoder mode
    @param chla_lagged_idx: [int] Index of chla_lagged in decoder features (required if autoregressive=True)
    @param chla_unc_idx: [int] Index of chla_uncertainty_lagged in decoder features (required if autoregressive=True)
    @param ar_scaling_params: [dict] Scaling params for autoregressive mode with keys:
                              target_mean, target_std, decoder_chla_mean, decoder_chla_std,
                              decoder_unc_mean, decoder_unc_std
    @return: [dict] validation metrics per horizon and overall
    """
    if autoregressive and (chla_lagged_idx is None or chla_unc_idx is None):
        raise ValueError("chla_lagged_idx and chla_unc_idx must be provided when autoregressive=True")
    if autoregressive and ar_scaling_params is None:
        raise ValueError("ar_scaling_params must be provided when autoregressive=True")

    # Track loss per horizon
    horizon_losses = {h: [] for h in range(decoder_seq_len)}
    total_losses = []

    with torch.no_grad():
        for batch in dataloader:
            if use_obs_uncertainty:
                x_enc, x_dec, y, y_obs_pi90 = batch
                y_obs_pi90 = y_obs_pi90.to(device)
            else:
                x_enc, x_dec, y = batch

            x_enc = x_enc.to(device)
            x_dec = x_dec.to(device)
            y = y.to(device)

            # Forward pass
            if autoregressive:
                output, _ = model.forward_autoregressive(
                    x_enc, x_dec,
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
                output, _ = model(x_enc, x_dec)

            # Overall loss
            if use_obs_uncertainty and head == 'CMAL':
                total_loss = MaskedCMALLoss_with_obs_uncertainty(
                    y, y_obs_pi90, output, n_mc_samples=n_mc_samples
                )
            else:
                total_loss = loss_function(y, output)
            total_losses.append(total_loss.item())

            # Per-horizon loss (for tracking purposes)
            for h in range(decoder_seq_len):
                # Extract output for this horizon
                output_h = {k: v[:, h:h+1, :] for k, v in output.items()}
                y_h = y[:, h:h+1, :]

                if use_obs_uncertainty and head == 'CMAL':
                    y_obs_pi90_h = y_obs_pi90[:, h:h+1, :]
                    loss_h = MaskedCMALLoss_with_obs_uncertainty(
                        y_h, y_obs_pi90_h, output_h, n_mc_samples=n_mc_samples
                    )
                else:
                    loss_h = loss_function(y_h, output_h)
                horizon_losses[h].append(loss_h.item())

    results = {
        'total': np.mean(total_losses),
        'by_horizon': {h: np.mean(losses) for h, losses in horizon_losses.items()}
    }

    print(f"Valid loss: {results['total']:.2f}")
    horizon_str = ', '.join([f'{h+1}d:{results["by_horizon"][h]:.2f}' for h in range(min(3, decoder_seq_len))])
    print(f"  By horizon: [{horizon_str}]")

    return results


class EncoderDecoderDataset(Dataset):
    """PyTorch Dataset for encoder-decoder data."""

    def __init__(self, x_encoder, x_decoder, y, y_obs_pi90=None):
        """
        Initialize the dataset.

        @param x_encoder: [torch.Tensor] Encoder input, shape (n_samples, encoder_seq_len, n_encoder_features)
        @param x_decoder: [torch.Tensor] Decoder input, shape (n_samples, decoder_seq_len, n_decoder_features)
        @param y: [torch.Tensor] Target, shape (n_samples, decoder_seq_len, n_targets)
        @param y_obs_pi90: [torch.Tensor, optional] Observation uncertainty (PI90),
                           shape (n_samples, decoder_seq_len, n_targets)
        """
        self.x_encoder = x_encoder
        self.x_decoder = x_decoder
        self.y = y
        self.y_obs_pi90 = y_obs_pi90
        self.has_obs_uncertainty = y_obs_pi90 is not None

    def __len__(self):
        return len(self.x_encoder)

    def __getitem__(self, idx):
        if self.has_obs_uncertainty:
            return self.x_encoder[idx], self.x_decoder[idx], self.y[idx], self.y_obs_pi90[idx]
        return self.x_encoder[idx], self.x_decoder[idx], self.y[idx]


def train_torch_encoder_decoder(model,
                                loss_function,
                                optimizer,
                                x_encoder_train,
                                x_decoder_train,
                                y_train,
                                x_encoder_val=None,
                                x_decoder_val=None,
                                y_val=None,
                                y_obs_pi90_train=None,
                                y_obs_pi90_val=None,
                                batch_size=32,
                                max_epochs=100,
                                head='CMAL',
                                early_stopping_patience=50,
                                shuffle=True,
                                weights_file=None,
                                log_file=None,
                                device='cpu',
                                track_horizon_metrics=False,
                                decoder_seq_len=10,
                                use_obs_uncertainty=False,
                                n_mc_samples=50,
                                autoregressive=False,
                                chla_lagged_idx=None,
                                chla_unc_idx=None,
                                ar_scaling_params=None):
    """
    Training function for encoder-decoder LSTM architecture.

    @param model: [object] EncoderDecoderLSTM model
    @param loss_function: loss function
    @param optimizer: [object] chosen optimizer
    @param x_encoder_train: [torch.Tensor] Encoder inputs for training
    @param x_decoder_train: [torch.Tensor] Decoder inputs for training
    @param y_train: [torch.Tensor] Targets for training
    @param x_encoder_val: [torch.Tensor] Encoder inputs for validation
    @param x_decoder_val: [torch.Tensor] Decoder inputs for validation
    @param y_val: [torch.Tensor] Targets for validation
    @param y_obs_pi90_train: [torch.Tensor, optional] Observation uncertainty (PI90) for training
    @param y_obs_pi90_val: [torch.Tensor, optional] Observation uncertainty (PI90) for validation
    @param batch_size: [int] Batch size
    @param max_epochs: [int] Maximum number of epochs
    @param head: [str] Type of output head
    @param early_stopping_patience: [int] Epochs without improvement before stopping
    @param shuffle: [bool] Shuffle training data
    @param weights_file: [str] Path to save model weights
    @param log_file: [str] Path to save training log
    @param device: [str] 'cpu' or 'gpu'
    @param track_horizon_metrics: [bool] Track validation metrics per forecast horizon
    @param decoder_seq_len: [int] Length of decoder sequence (needed for horizon tracking)
    @param use_obs_uncertainty: [bool] Whether to use observation uncertainty in loss
    @param n_mc_samples: [int] Number of MC samples for observation uncertainty loss
    @param autoregressive: [bool] Whether to use autoregressive decoder mode
    @param chla_lagged_idx: [int] Index of chla_lagged in decoder features (required if autoregressive=True)
    @param chla_unc_idx: [int] Index of chla_uncertainty_lagged in decoder features (required if autoregressive=True)
    @param ar_scaling_params: [dict] Scaling params for autoregressive mode with keys:
                              target_mean, target_std, decoder_chla_mean, decoder_chla_std,
                              decoder_unc_mean, decoder_unc_std (required if autoregressive=True)
    @return: [object] Trained model
    """
    print(f"Training encoder-decoder on {device}")
    if use_obs_uncertainty:
        print(f"Using observation uncertainty in loss with {n_mc_samples} MC samples")
    if autoregressive:
        print(f"Using autoregressive decoder mode (chla_lagged_idx={chla_lagged_idx}, chla_unc_idx={chla_unc_idx})")
        if ar_scaling_params is None:
            raise ValueError("ar_scaling_params must be provided when autoregressive=True")
    print("Start training...", flush=True)

    if not early_stopping_patience:
        early_stopping_patience = max_epochs

    epochs_since_best = 0
    best_loss = float('inf')

    # Handle GPU
    if device == 'gpu':
        device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
    else:
        device = torch.device('cpu')

    model.to(device)

    # Create dataloaders
    train_dataset = EncoderDecoderDataset(
        x_encoder_train, x_decoder_train, y_train,
        y_obs_pi90=y_obs_pi90_train if use_obs_uncertainty else None
    )
    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=batch_size, shuffle=shuffle, pin_memory=True
    )

    if x_encoder_val is not None:
        val_dataset = EncoderDecoderDataset(
            x_encoder_val, x_decoder_val, y_val,
            y_obs_pi90=y_obs_pi90_val if use_obs_uncertainty else None
        )
        val_loader = torch.utils.data.DataLoader(
            val_dataset, batch_size=batch_size, shuffle=False, pin_memory=True
        )

    val_time = []
    train_time = []

    # Training log
    log_cols = ['epoch', 'loss', 'val_loss', 'time', 'val_time']
    train_log = pd.DataFrame(columns=log_cols)

    for i in range(max_epochs):
        t1 = time.time()

        model.train()
        epoch_loss = train_loop_encoder_decoder(
            i, train_loader, head, model, loss_function, optimizer, device,
            use_obs_uncertainty=use_obs_uncertainty, n_mc_samples=n_mc_samples,
            autoregressive=autoregressive, chla_lagged_idx=chla_lagged_idx, chla_unc_idx=chla_unc_idx,
            ar_scaling_params=ar_scaling_params
        )
        train_time.append(time.time() - t1)

        train_log = pd.concat([
            train_log,
            pd.DataFrame([[i, epoch_loss, np.nan, time.time()-t1, np.nan]], columns=log_cols, index=[i])
        ])

        # Validation
        if x_encoder_val is not None:
            s1 = time.time()
            model.eval()

            if track_horizon_metrics:
                val_results = val_loop_encoder_decoder_by_horizon(
                    val_loader, head, model, loss_function, decoder_seq_len, device,
                    use_obs_uncertainty=use_obs_uncertainty, n_mc_samples=n_mc_samples,
                    autoregressive=autoregressive, chla_lagged_idx=chla_lagged_idx, chla_unc_idx=chla_unc_idx,
                    ar_scaling_params=ar_scaling_params
                )
                epoch_val_loss = val_results['total']
            else:
                epoch_val_loss = val_loop_encoder_decoder(
                    val_loader, head, model, loss_function, device,
                    use_obs_uncertainty=use_obs_uncertainty, n_mc_samples=n_mc_samples,
                    autoregressive=autoregressive, chla_lagged_idx=chla_lagged_idx, chla_unc_idx=chla_unc_idx,
                    ar_scaling_params=ar_scaling_params
                )

            if epoch_val_loss < best_loss:
                torch.save(model.state_dict(), weights_file)
                best_loss = epoch_val_loss
                epochs_since_best = 0
            else:
                epochs_since_best += 1

            if epochs_since_best > early_stopping_patience:
                print(f"Early Stopping at Epoch {i}")
                break

            train_log.loc[train_log.epoch == i, "val_loss"] = epoch_val_loss
            train_log.loc[train_log.epoch == i, "val_time"] = time.time() - s1
            val_time.append(time.time() - s1)

    if log_file:
        train_log.to_csv(log_file)

    if x_encoder_val is None:
        torch.save(model.state_dict(), weights_file)
        print("Average Training Time: {:.4f} secs/epoch".format(np.mean(train_time)))
    else:
        print("Average Training Time: {:.4f} secs/epoch".format(np.mean(train_time)))
        print("Average Validation (Inference) Time: {:.4f} secs/epoch".format(np.mean(val_time)))

    # Move back to CPU
    model.to('cpu')
    return model


def train_torch(model,
                loss_function,
                optimizer,
                x_train,
                y_train,
                h_train,
                c_train,
                h_val,
                c_val,
                weighting_matrix_train,
                weighting_matrix_val,
                batch_size,
                max_epochs,
                head,
                umal_extend_batch,
                umal_n_taus_train,
                umal_tau_min,
                umal_tau_max,
                weight_loss,
                weight_threshold,
                weight_value,
                early_stopping_patience=False,
                x_val = None,
                y_val = None,
                shuffle = False,
                weights_file = None,
                log_file= None,
                device = 'cpu',
                keep_portion = None):
    """
    modified from river-dl
    @param model: [objetct] initialized torch model
    @param loss_function: loss function
    @param optimizer: [object] chosen optimizer
    @param x_train:
    @param batch_size: [int]
    @param max_epochs: [maximum number of epochs to run for]
    @param early_stopping_patience: [int] number of epochs without improvement in validation loss to run before stopping training
    @param shuffle: [bool] Shuffle training batches
    @param weights_file: [str] path save trained model weights
    @param log_file: [str] path to save training log to
    @return: [object] trained model
    """

    print(f"Training on {device}")
    print("start training...",flush=True)

    if not early_stopping_patience:
        early_stopping_patience = max_epochs

    epochs_since_best = 0
    best_loss = 100000000 # Will get overwritten

    if keep_portion is not None:
        if keep_portion > 1:
            period = int(keep_portion)
        else:
            period = int(keep_portion * y_train.shape[1])
        y_train[:, :-period, ...] = np.nan
        if y_val is not None:
            y_val[:, :-period, ...] = np.nan

    if device == 'gpu':
        device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
        model.to(device)
        x_train = x_train.to(device)
        y_train = y_train.to(device)
        x_val = x_val.to(device)
        y_val = y_val.to(device)
        h_train = h_train.to(device)
        c_train = c_train.to(device)
        h_val = h_val.to(device)
        c_val = c_val.to(device)
        weighting_matrix_train = weighting_matrix_train.to(device)
        weighting_matrix_val = weighting_matrix_val.to(device)

    # Put together dataloaders
    train_data = []
    for i in range(len(x_train)):
        train_data.append([x_train[i], y_train[i]])

    train_loader = torch.utils.data.DataLoader(train_data, batch_size=batch_size, shuffle=shuffle, pin_memory=True)

    if x_val is not None:
        val_data = []
        for i in range(len(x_val)):
            val_data.append([x_val[i], y_val[i]])

        val_loader = torch.utils.data.DataLoader(val_data, batch_size=batch_size, shuffle=shuffle, pin_memory=True)

    val_time = []
    train_time = []

    ### Run training loop
    log_cols = ['epoch', 'loss', 'val_loss','time','val_time']
    train_log = pd.DataFrame(columns=log_cols)

    for i in range(max_epochs):

        t1 = time.time()

        model.train()
        epoch_loss = train_loop(i, train_loader, h_train, c_train, weighting_matrix_train,
                                head, model, loss_function, optimizer, umal_extend_batch,
                                umal_n_taus_train, umal_tau_min, umal_tau_max,
                                weight_loss, weight_threshold, weight_value, device)
        train_time.append(time.time() - t1)
        train_log = pd.concat([train_log,pd.DataFrame([[i, epoch_loss, np.nan,time.time()-t1,np.nan]],columns=log_cols,index=[i])])

        #Val
        if x_val is not None:
            s1 = time.time()
            model.eval()
            epoch_val_loss = val_loop(val_loader, h_val, c_val, weighting_matrix_val,
                                      head, model, loss_function, umal_extend_batch,
                                      umal_n_taus_train, umal_tau_min, umal_tau_max,
                                      weight_loss, weight_threshold, weight_value, device)

            if epoch_val_loss < best_loss:
                torch.save(model.state_dict(), weights_file)
                best_loss = epoch_val_loss
                epochs_since_best = 0
            else:
                epochs_since_best += 1
            if epochs_since_best > early_stopping_patience:
                print(f"Early Stopping at Epoch {i}")
                break
            train_log.loc[train_log.epoch==i,"val_loss"]=epoch_val_loss
            train_log.loc[train_log.epoch==i,"val_time"]=time.time() - s1
            val_time.append(time.time()-s1)

    train_log.to_csv(log_file)
    #print(train_log)
    if x_val is None:
        torch.save(model.state_dict(), weights_file)
        print("Average Training Time: {:.4f} secs/epoch".format(np.mean(train_time)))
    else:
        print("Average Training Time: {:.4f} secs/epoch".format(np.mean(train_time)))
        print("Average Validation (Inference) Time: {:.4f} secs/epoch".format(np.mean(val_time)))
    # move back to cpu when complete for simplicity
    model.to('cpu')
    return model
