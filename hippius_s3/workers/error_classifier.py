from hippius_s3.workers.uploader import classify_error


def classify_unpin_error(error: Exception) -> str:
    """Classify errors specifically for unpin operations.

    Key difference: 404 is TRANSIENT for unpins (pin not complete yet).
    For all other errors, delegates to the shared classify_error function.
    """
    err_str = str(error).lower()

    if hasattr(error, "response") and hasattr(error.response, "status_code") and error.response.status_code == 404:
        return "transient"

    if "404" in err_str or "not found" in err_str:
        return "transient"

    return classify_error(error)
