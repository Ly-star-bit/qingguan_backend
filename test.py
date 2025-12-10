import os


template_dir = os.path.join(os.getcwd(), "file/excel_template")
excel_path = next(
    (
        os.path.join(template_dir, f)
        for f in os.listdir(template_dir)
        if f.startswith("LAX发出-HTT")
    ),
    None,
)
print(excel_path)