from pipeline.data import input
from pipeline import pipeline_wrapper

def main() -> None:

    print("Loading config file")
    config = input.load_json_config_file(".\\config.json")

    pipeline_wrapper.run(config)

if __name__ == "__main__":
    main()