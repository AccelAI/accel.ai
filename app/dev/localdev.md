#Python Flask Application

Developed to Integrate with Open-sourced LMS & Medium Blog API

##Quickstart

1. Clone this repo onto your local environment using `git clone https://github.com/quickresolve/accel.ai.git`

2. Change directory into application `cd accel.ai`

3. Activate the virtual environment `source flask-aws/bin/activate`

4. Then install the packages needed for this application `pip install -r requirements.txt`

5. Start your development server by running `python application.py`. Your server should be running on `localhost:8080`.


##Styling

Original design of this application was built off of [Material Design One Page HTML Template](https://github.com/joashp/material-design-template).

Changes to styling of template pages should be added to "custom.css" or "custom.js" files located in the static directory, unless otherwise approved for a component based system.



##Deploying to AWS

Only if you have the correct permissions in place may you deploy production code of this application to AWS.

*Must be granted access to AWS account

*Must have proper installation of elastic bean stalk

*Must have verified aws-access-id & secret-key

For those with the proper persmissions in place, updates can be made with `eb deploy`.




##License

[MIT License](/LICENSE).



