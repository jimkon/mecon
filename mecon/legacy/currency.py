

def GBP_to_curr_exchange_rate(curr, date=None):
    gbp_exchange_rate_dict = {
        'EUR': 0.88,
        'GBP': 1,
        'HUF': 0.0021,
        'USD': 0.89
    }
    if curr not in gbp_exchange_rate_dict.keys():
        # print(f"Conversion rate for {curr} not found. Amount will be nullified")
        # return .0
        raise ValueError(f"Conversion rate for {curr} not found. Amount will be nullified")

    return gbp_exchange_rate_dict[curr]
