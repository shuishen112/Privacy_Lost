# stastics_cal

# python pipeline_plot_picture/stastics_cal_12.py
# python pipeline_plot_picture/stastics_cal_45_1.py
# python pipeline_plot_picture/stastics_ca_45_2.py
# python pipeline_plot_picture/plot_figure12.py
# python pipeline_plot_picture/plot_experiment.py


# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2020 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2019 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2018 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2017 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2016 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2015 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2014 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2013 2>&1 &
# nohup python pipeline_plot_picture/stastics_cal_categories.py --year=2012 2>&1 &

# nohup python pipeline_archived/collecting_trackers_from_cc.py --year=2021  > collecting_data_from_CC_all_2021.log 2>&1 &
export PYTHONPATH=./
python pipeline_archived/collecting_trackers_from_cc.py --wandb --year=2015 --input_path=communication_conference/CC/historical_scan/6.5_snapshot_2015_host_name.csv --num_process=96 --multi_process --output_dir=communication_conference/CC/historical_trackers/6.5_2015.json > collect_trackers_2015.log
# python pipeline_archived/collecting_trackers_from_cc.py --wandb --year=2016 --input_path=communication_conference/CC/historical_scan/6.5_snapshot_2016_host_name.csv --num_process=96 --multi_process --output_dir=communication_conference/CC/historical_trackers > collect_trackers_2016.log
# python pipeline_archived/collecting_trackers_from_cc.py --wandb --year=2017 --input_path=communication_conference/CC/historical_scan/6.5_snapshot_2017_host_name.csv --num_process=96 --multi_process --output_dir=communication_conference/CC/historical_trackers > collect_trackers_2017.log