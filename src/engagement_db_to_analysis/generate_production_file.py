from core_data_modules.traced_data.io import TracedDataCSVIO


def generate_production_file(messages_td, production_csv_output_path):
    production_keys = ["participant_uuid", "dataset", "text"]
    with open(production_csv_output_path, "w") as f:
        TracedDataCSVIO.export_traced_data_iterable_to_csv(messages_td, f, production_keys)
