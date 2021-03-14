from preprocess_trade_data import transform_format
from calc_earning_at_risk import load_calculate_summarize
from calc_statistics import get_output, get_hh_traces
from multiprocessing import Pool


if __name__ == '__main__':
    transform_format(job_id=50002,
                     date_input='2021-02-12',
                     filename='Deal Capture (2021-01-14)_Job34.xlsx',
                     sheet_name='Position Output',
                     start_year=2021, start_month=1, start_day=1,
                     end_year=2022, end_month=2, end_day=15)
    run_id = 10072
    total_num_sims = 915
    job_id = 50002
    date_input = '2021-02-12'
    start_year = 2021
    start_month = 1
    start_day = 1
    end_year = 2022
    end_month = 2
    end_day = 15
    p = Pool()
    for sim_index in range(total_num_sims):
        p.apply_async(load_calculate_summarize, args=(run_id,
                                                      job_id,
                                                      date_input,
                                                      sim_index,
                                                      start_year,
                                                      start_month,
                                                      start_day,
                                                      end_year,
                                                      end_month,
                                                      end_day, ))
        # load_calculate_summarize(run_id,
        #                          job_id,
        #                          date_input,
        #                          sim_index,
        #                          start_year,
        #                          start_month,
        #                          start_day,
        #                          end_year,
        #                          end_month,
        #                          end_day)
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    get_output(run_id, job_id, total_num_sims)
    get_hh_traces(run_id, job_id)
