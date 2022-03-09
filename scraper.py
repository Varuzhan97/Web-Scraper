import yaml
import os
import requests
import csv
import threading
from datetime import datetime

def load_configs(config_dir):
    #Load YAML file
    config_file_path = os.path.join(config_dir, "config.yaml")
    config_file =  open(config_file_path, 'r')
    main_config = yaml.safe_load(config_file)

    configs = dict()
    #Get configurations
    configs["Postcodes"] = main_config["Postcodes"]
    configs["Items Link"] = main_config["Items Link"]
    configs["Items Count Link"] = main_config["Items Count Link"]
    configs["Item Base Link"] = main_config["Item Base Link"]
    configs["Image Base Link"] = main_config["Image Base Link"]
    configs["Postcode Mark"] = main_config["Postcode Mark"]
    configs["Search Limit Mark"] = main_config["Search Limit Mark"]
    configs["Action Mark"] = main_config["Action Mark"]
    configs["Actions"] = main_config["Actions"]

    config_file.close()
    return configs

def get_response(link, stream = False):
    headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
    response = requests.get(link, headers = headers, stream = stream)
    return response

#Get searched items count
def get_items_count(link):
    response = get_response(link)
    data = response.json()
    return str(data['count'])

#Check output folder
#If not exist then create the folder
def check_dir(folder_dir):
    if not os.path.isdir(folder_dir):
        os.makedirs(folder_dir)
    return folder_dir

def write_to_csv(data, postcode, output_folder_dir):
    print('Generating CSV file for the postcode {}.'.format(postcode))
    csv_header = ["URL", "ID", "Address", "Property type", "Number of bedrooms", "Price", "Features"]

    file_name = postcode
    csv_file_path = os.path.join(output_folder_dir, file_name + ".csv")

    with open(csv_file_path, 'w') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter = ',')
        csvwriter.writerow(csv_header)
        for line in data:
            csvwriter.writerow(line)

def download_images(images, images_folder, image_base_link):
    for i in range(len(images)):
        #No need to use urlparse library to join URL's
        img_link = image_base_link + images[i]
        img_data = get_response(img_link, stream = True)
        with open(os.path.join(images_folder,  str(i) + '.jpg'), 'wb') as handler:
            handler.write(img_data.content)

def process_items(download_link, raw_item_base_link, raw_image_base_link, output_folder_dir, ps):
    #Get link data
    response = get_response(download_link)
    data = response.json()

    #Check output dir
    check_dir(output_folder_dir)
    #List for storing necessary data
    row = list()

    threads=[]

    for item in data.get("listings"):
        #Get item url, id, address, property type, number of bedrooms, price, features.
        url = raw_item_base_link + item.get("listingAliasId")
        id = item.get("listingAliasId")
        address = item.get("displayAddress")
        property_type = item.get("propertyType")
        number_of_bedrooms = item.get("bedrooms")
        price = item.get("price")
        features = item.get("keyFeatures")
        #Convert features list to string
        features_string = ""
        if features is not None:
            for f in features:
                features_string += (f + ".")

        images_folder = os.path.join(output_folder_dir, id)
        #Detect duplicates
        if os.path.exists(images_folder):
            print("Detected duplicated item with ID: {}. Skipping this item.".format(id))
            continue

        row.append([url, id, address, property_type, number_of_bedrooms, price, features_string])

        #Process images
        #Get other images
        images = item.get("imageUrls")
        #Append primary image to images list
        images.append(item.get("primaryImage"))

        check_dir(images_folder)

        download_thread = threading.Thread(target=download_images, args=(images, images_folder, raw_image_base_link))
        download_thread.start()
        threads.append(download_thread)

    for thread in threads:
        thread.join()

    write_to_csv(row, ps, output_folder_dir)


if __name__ == "__main__":
    start_time=datetime.now()

    #Get working directory where are the config file and output location
    working_dir = os.getcwd()

    #Get configs
    configs = load_configs(working_dir)

    for pc in configs.get("Postcodes"):
        for action_key, action_value in configs.get("Actions").items():
            #Create items counter link
            #Replace postcode mark with postcode value
            raw_items_count_link = configs.get("Items Count Link")
            count_link =  raw_items_count_link.replace(configs.get("Postcode Mark"), pc)
            #Replace action mark with action value
            count_link =  count_link.replace(configs.get("Action Mark"), action_value)

            #Get results count for postcode and action(buy or rent)
            count = get_items_count(count_link)

            #After getting searched items count, process download link
            #Replace postcode mark with postcode value
            raw_link = configs.get("Items Link")
            download_link =  raw_link.replace(configs.get("Postcode Mark"), pc)
            #Replace action mark with action value
            download_link =  download_link.replace(configs.get("Action Mark"), action_value)
            #Replace search limit mark with search limit value
            download_link =  download_link.replace(configs.get("Search Limit Mark"), count)
            print("Processing postcode \"{}\", type is \"{}\", found {} properties.".format(pc, action_key.lower(), count))
            #Start items processing
            output_dir = os.path.join(working_dir, "inDATAside", action_key)
            process_items(download_link, configs.get("Item Base Link"), configs.get("Image Base Link"), output_dir, pc)

    end_time=datetime.now()
    exec_time= end_time-start_time
    print("Total execution time for the function {}".format(exec_time))
