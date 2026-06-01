from datetime import timedelta, datetime

# Ejemplo del formato de la duración que nos llega: P1DT2H30M45S
def parse_duration(duration_str):
    duration_str = duration_str.replace('P', '').replace('T', '') # 1D2H30M45S

    duration_components = ('D', 'H', 'M', 'S')
    duration_values = {'D': 0, 'H': 0, 'M': 0, 'S': 0}
    
    for duration_component in duration_components:
        if duration_component in duration_str:
            duration_value, duration_str = duration_str.split(duration_component)
            duration_values[duration_component] = int(duration_value)

    total_duration = timedelta(
        days=duration_values['D'],
        hours=duration_values['H'],
        minutes=duration_values['M'],
        seconds=duration_values['S']
    )

    return total_duration


def transform_data(row):
    video_duration_timedelta = parse_duration(row['duration'])

    # datetime.min -> 00:00:00
    row['duration'] = (datetime.min + video_duration_timedelta).time()

    row['type'] = 'Shorts' if video_duration_timedelta.total_seconds() <= 60 else 'Normal'

    return row