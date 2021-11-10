echo "ls"
ls

# build docker image
echo -e "\e[1;97;40m Building Docker Images \e[0m"
docker compose up --build

# run unit tests inside docker container
#echo -e "\e[1;97;40m Running unit tests \e[0m"
#docker run --entrypoint "coverage" stewart-morgridge/fast_km:dev run -m pytest ./ -rP

# run unit tests outside docker container (api queries)
