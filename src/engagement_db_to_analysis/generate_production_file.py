from core_data_modules.traced_data import Metadata, TracedData
from core_data_modules.traced_data.io import TracedDataCSVIO
from core_data_modules.util import TimeUtils

from src.engagement_db_to_analysis.configuration import DatasetTypes


def generate_production_file(user, analysis_dataset_config, participants_traced_data_map, analysis_output_dir):
    demographic_datasets, rqa_datasets = [], []
    for config in analysis_dataset_config:
        if config.dataset_type == DatasetTypes.DEMOGRAPHIC:
            demographic_datasets.extend(config.engagement_db_datasets)
        if config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            rqa_datasets.extend(config.engagement_db_datasets)

    headers = ["uid", *demographic_datasets, *rqa_datasets]
    traced_data_iterable = []

    for uuid, dataset_messages_map in participants_traced_data_map.items():
        demog_columns, rqa_columns = {}, {}
        
        demog_columns["uid"] = uuid
        for dataset, messages in dataset_messages_map.items():
            if dataset in demographic_datasets:
                demographic_answers = []
                for msg in messages[0]:
                    demographic_answers.append(msg['Data']['text'])
                demog_columns.update({dataset: ";".join(demographic_answers)})

        for dataset, messages in dataset_messages_map.items():
            if dataset in rqa_datasets:
                for msg in messages[0]:
                    rqa_columns = {dataset: msg['Data']['text']}
                    rqa_columns.update(demog_columns)
                    traced_data_iterable.append(TracedData(rqa_columns, Metadata(
                        user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))

    with open(f"{analysis_output_dir}/production.csv", "w") as f:
        TracedDataCSVIO.export_traced_data_iterable_to_csv(traced_data_iterable, f, headers=headers)
