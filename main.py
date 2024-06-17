import pandas as pd
from download import download_pdf  # Assuming you have a module named download
from infoExtract import read_pdf, extract_info  # Assuming you have these modules
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os
import time
from helper import clear_temp_folders


def initialize_directories():
    if not os.path.exists("temp"):
        os.makedirs("temp")


def read_original_dataframe(filepath, num_rows=25):
    df = pd.read_csv(filepath)[:num_rows]
    df = df.drop_duplicates(subset='Insc_Cadastral')
    return df


def wait_before_start(n=3):
    for i in range(n):
        print(f"Waiting {n - i} seconds...")
        time.sleep(1)
    print("Starting")


def process_item(item):
    number = item["Insc_Cadastral"].replace(' ', '')
    try:
        response = download_pdf(number)
    except Exception as e:
        print(f"Error downloading PDF for {number}: {e}")
        return None

    if response:
        file_path = f"temp/{number}/{number}.pdf"
        pages_content = read_pdf(file_path)
        if pages_content:
            info = extract_info(pages_content[0][pages_content[0].find("IMÓVEL:"):])
            info['Insc_Cadastral'] = item["Insc_Cadastral"]
            return item['index'], info
    return None


def save_and_clear(df, cycle_number):
    df.to_csv(f"temp/BaseConsolidada_cycle{cycle_number}.csv", index=False)
    df.to_excel(f"temp/BaseConsolidada_cycle{cycle_number}.xlsx", index=False)
    df.drop(df.index, inplace=True)


def process_dataframe_chunks(df_original, save_number, concurrent_threads):
    num_chunks = len(df_original) // save_number + (len(df_original) % save_number > 0)
    all_results = []
    last_cycle = -1

    for cycle in range(num_chunks):
        start_time_cycle = time.time()
        print(f"Starting cycle {cycle + 1}")
        start_index = cycle * save_number
        end_index = min((cycle + 1) * save_number, len(df_original))
        df_chunk = df_original.iloc[start_index:end_index]

        current_threads = []

        with ThreadPoolExecutor(max_workers=concurrent_threads) as executor:
            for index, row in df_chunk.iterrows():
                item = {"index": index, "Insc_Cadastral": row["Insc_Cadastral"]}
                current_threads.append(executor.submit(process_item, item))

        for thread in threading.enumerate():
            if thread != threading.current_thread():
                thread.join()

        for future in as_completed(current_threads):
            result = future.result()
            if result is not None:
                index, info = result
                all_results.append(info)

        save_cycle_results(all_results, cycle)
        last_cycle += 1
        combined_df = combine_csv_files(last_cycle)
        merge_and_save_intermediate_dataframe(combined_df, df_original, cycle)
        all_results = []

        end_time_cycle = time.time()
        clear_temp_folders()
        print(f"Cycle {cycle + 1} completed in {end_time_cycle - start_time_cycle:.2f} seconds")

    return last_cycle


def save_cycle_results(results, cycle):
    df = pd.DataFrame(results)
    csv_filename = f'temp/BaseConsolidada_output_{cycle}.csv'
    excel_filename = f'temp/BaseConsolidada_output_{cycle}.xlsx'
    df.to_csv(csv_filename, index=False)
    df.to_excel(excel_filename, index=False)
    print(f"CSV file created: {csv_filename}")
    print(f"Excel file created: {excel_filename}")


def combine_csv_files(last_cycle):
    combined_df = pd.concat([pd.read_csv(f"temp/BaseConsolidada_output_{i}.csv") for i in range(last_cycle + 1)])
    combined_df.to_csv("temp/BaseConsolidada_combined.csv", index=False)
    print("Combined CSV file created: temp/BaseConsolidada_combined.csv")
    return combined_df


def merge_and_save_intermediate_dataframe(combined_df, df_original, cycle):
    merged_df = pd.merge(combined_df, df_original, on="Insc_Cadastral", how="outer", suffixes=('_new', '_original'))

    selected_columns = [
        'Cod_Reduzido', 'Insc_Cadastral', 'Imovel_Endereco', 'Bairro', 'Quadra', 'Lote',
        'Area Territorial', 'Area Predial', 'Testada', 'Cod_Prefeitura', 'Contribuinte_CPF',
        'Contribuinte_Nome', 'Contribuinte_Endereco', 'Contribuinte_CEP', 'Bairro_Contribuinte'
    ]

    for col in selected_columns:
        col_new = f'{col}_new'
        col_original = f'{col}_original'
        if col_new in merged_df.columns and col_original in merged_df.columns:
            merged_df[col] = merged_df[col_new].combine_first(merged_df[col_original])
        elif col_new in merged_df.columns:
            merged_df[col] = merged_df[col_new]
        elif col_original in merged_df.columns:
            merged_df[col] = merged_df[col_original]

    final_columns = [
        'Cod_Reduzido', 'Insc_Cadastral', 'Imovel_Endereco', 'Bairro', 'Quadra', 'Lote',
        'Area Territorial', 'Area Predial', 'Testada', 'Cod_Prefeitura', 'Contribuinte_CPF',
        'Contribuinte_Nome', 'Contribuinte_Endereco', 'Contribuinte_CEP', 'Bairro_Contribuinte', 'CEPImovel'
    ]
    final_columns = [col for col in final_columns if col in merged_df.columns]

    final_df = merged_df[final_columns]
    final_df.to_csv(f"temp/Final_Combined_cycle{cycle}.csv", index=False)
    final_df.to_excel(f"temp/Final_Combined_cycle{cycle}.xlsx", index=False)
    print(f"Final combined CSV file for cycle {cycle} created: temp/Final_Combined_cycle{cycle}.csv")


def main():
    start_time = time.time()
    initialize_directories()
    df_original = read_original_dataframe("BaseConsolidada.csv")
    concurrent_threads = 15
    wait_before_start()
    last_cycle = process_dataframe_chunks(df_original, concurrent_threads, concurrent_threads)
    combined_df = combine_csv_files(last_cycle)
    merge_and_save_intermediate_dataframe(combined_df, df_original, "final")
    time_taken = time.time() - start_time
    print(f"Time taken: {time_taken:.2f} seconds")


if __name__ == "__main__":
    main()
