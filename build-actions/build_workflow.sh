# build docker image
echo -e "\e[1;97;40m Building Docker Image \e[0m"
docker build -t stewart-morgridge/fast_km:dev .

# run unit tests inside docker container
echo -e "\e[1;97;40m Running unit tests \e[0m"
docker run --entrypoint "coverage" stewart-morgridge/fast_km:dev run -m pytest ./ -rP
docker run --entrypoint "coverage" stewart-morgridge/fast_km:dev run report
