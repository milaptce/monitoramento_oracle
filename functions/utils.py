import configparser
import os
from datetime import datetime

def check_first_run():
    config_file = "config/execucao.ini"
    config = configparser.ConfigParser()
    
    if not os.path.exists(config_file):
        config["Execution"] = {
            "FIRST_RUN": "1",
            "LAST_RUN_TIMESTAMP": datetime.now().isoformat()
        }
        with open(config_file, "w") as f:
            config.write(f)
        return True
    
    config.read(config_file)
    first_run = config.get("Execution", "FIRST_RUN", fallback="1")
    
    if first_run == "1":
        config.set("Execution", "FIRST_RUN", "0")
        config.set("Execution", "LAST_RUN_TIMESTAMP", datetime.now().isoformat())
        with open(config_file, "w") as f:
            config.write(f)
        return True
    
    return False


def schedule_next_run(hours=6):
    next_time = datetime.now().strftime("%H:%M")
    logging.info(f"🕒 Próxima execução agendada em {hours} horas (aproximadamente às {next_time}).")
    print(f"🕒 Próxima execução agendada em {hours} horas (aproximadamente às {next_time}).")