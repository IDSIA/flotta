from ferdelance.core.queries import QueryFeature


def convert_features_in_to_list(features_in: QueryFeature | list[QueryFeature] | None = None) -> list[QueryFeature]:
    """Sanitize the input list of features in a list of QueryFeature.

    Args:
        features_in (QueryFeature | list[QueryFeature] | None, optional):
            List of features. These can be a QueryFeature, a list of QueryFeature, or None.
            Defaults to None.

    Returns:
        list[QueryFeature]:
            The input converted in a list of QueryFeatures.
    """
    if features_in is None:
        return list()

    if isinstance(features_in, QueryFeature):
        features_in = [features_in]

    return features_in


def convert_features_out_to_list(
    features_in: list[QueryFeature],
    features_out: QueryFeature | list[QueryFeature] | str | list[str] | None = None,
    check_len: bool = True,
) -> list[QueryFeature]:
    """Sanitize the output list of features in a list of QueryFeature.

    Args:
        features_in (list[QueryFeature]): _description_
        features_out (QueryFeature | list[QueryFeature] | str | list[str] | None, optional):
            List of features used as output. If None is passed, then the output features will be a copy of
            the features_in parameter. If a string or a list of string is passed, then a new list of outputs
            will be generated with the given name(s) and the same dtype as features_in (where possible). If
            an empty list is passed, it will be returned another empty list.
            Defaults to None.

    Returns:
        list[QueryFeature]:
            The outputs converted in a list of QueryFeatures.

    Raises:
        ValueError:
    """

    if features_out is None:
        return features_in.copy()

    if isinstance(features_out, str):
        if len(features_in) != 1:
            raise ValueError("Multiple input features but only one feature as output.")
        return [QueryFeature(name=features_out, dtype=features_in[0].name)]

    if isinstance(features_out, QueryFeature):
        return [features_out]

    if isinstance(features_out, list):
        if len(features_out) == 0:
            return list()

        if check_len and len(features_in) != len(features_out):
            raise ValueError("Different number of input features and output features.")

        ret: list[QueryFeature] = list()

        for f_in, f_out in zip(features_in, features_out):
            if isinstance(f_out, QueryFeature):
                ret.append(f_out)
            else:
                ret.append(QueryFeature(name=f_out, dtype=f_in.dtype))

        return ret

    raise ValueError("Unsupported features_output parameter type.")
