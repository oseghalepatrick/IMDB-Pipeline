Welcome to your dbt project! To start [Sign up here](https://www.getdbt.com/)


Follow the setup instruction [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md)

Immediately after the setup, click on `Start Developing` and then `initialize project` to start developing.

A project includes the following files:

1. `dbt_project.yml`: file used to configure the dbt project.
2. *.yml files under folders models, data, macros: documentation files.
3. Files inside folder models: The sql files contain the scripts to run our models, this will cover staging, core and a datamarts models. At the end, these models will follow this structure:

<img src="images/dbt.png">

Now that the development is done, its time to setup the `production environment`.
1. Click on `New Envirinment` from the menu and rename as `Production`
2. Then, type the deployment database and click set environment.
3. Set the `Run Job` and set the trigger schedule.


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

[Back to home page](./project/README.md)