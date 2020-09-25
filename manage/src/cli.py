import typer
from main import (
    prepare_local_resources,
    setup_cloud_resources,
    hydrate_cloud_resources,
    monitor_remote_ops,
    # media_sync,
    s3_clean,
    statistics,
)
from init import log_files
from s3 import medias_copy
import os
import time
from tabulate import tabulate
from local import process_local_movie_medias


app = typer.Typer()


@app.command()
def run_all(tic=time.perf_counter()):
    """ Run the whole stack """
    cloud_setup(tic)
    build_local(tic)
    cloud_hydrate(tic)
    finalize(tic)
    monitor_remote_ops()
    typer.echo("- All tasks executed successfully -")


@app.command()
def build_local(tic=time.perf_counter()):
    """ Get local media files """
    prepare_local_resources()
    finalize(tic)


@app.command()
def cloud_setup(tic=time.perf_counter()):
    """ Initiate cloud resources for the project """
    setup_cloud_resources()
    finalize(tic)


@app.command()
def cloud_hydrate(tic=time.perf_counter()):
    """ Hydrate cloud resources """
    hydrate_cloud_resources()
    finalize(tic)


@app.command()
def assets_sync(tic=time.perf_counter()):
    """ Synchronize media assets """
    # media_sync()
    medias_copy()
    finalize(tic)


@app.command()
def process_movies(tic=time.perf_counter()):
    """ Get movie medias from local list and transcode them """
    process_local_movie_medias()
    finalize(tic)


@app.command()
def clear_logs(tic=time.perf_counter(), log_files=log_files):
    """ Clear all logs """
    typer.echo("Clearing log files...")
    for log_file in log_files:
        if os.path.exists(log_file):
            with open(log_file, "r+") as t:
                t.truncate(0)
        else:
            pass
    typer.echo("...done.")
    finalize(tic)


@app.command()
def clean_bucket(tic=time.perf_counter()):
    """ Clean incomplete multipart uploads """
    typer.echo("Cleaning bucket...")
    run = s3_clean()
    if run:
        typer.echo("...done.")
    else:
        typer.echo("...aborted!")
    finalize(tic)


def finalize(tic):
    toc = time.perf_counter()
    if statistics:
        typer.echo(tabulate(statistics))
    else:
        pass
    typer.echo(f"Executed task(s) in {toc - tic:0.4f} second(s).")


if __name__ == "__main__":
    app()
