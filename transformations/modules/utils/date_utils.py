from datetime import date
from dateutil.relativedelta import relativedelta

def get_target_yyyymm(months_ago=2):

    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime("%Y-%m")


def get_month_start_n_months_ago(months_ago: int = 2) -> date:
  
    return date.today().replace(day=1) - relativedelta(months=months_ago)