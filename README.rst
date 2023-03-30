=========
pullshift
=========


.. image:: https://img.shields.io/pypi/v/pullshift.svg
        :target: https://pypi.python.org/pypi/pullshift

.. image:: https://readthedocs.org/projects/pullshift/badge/?version=latest
        :target: https://pullshift.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status




Like Pushshift_'s API, but slower and not in the cloud.

Download and extract data from Pushshift's `archive files`_.

*This project is WIP*
---------
Installation
---------
.. code-block::

    pip install -r requirements.txt
    python -m spacy download en_core_web_lg

TODO
---------
- [ ] parallelize downloads
  - [ ] fix tqdm for parallel downloads
- [ ] config for file locations and naming
- [ ] cli for specifying download options
  - [ ] date ranges
    - a single year means the whole year,
    - full start date and no end date means just one file
  - [ ] overwrite
  - [ ] download, decompress, and filter on the fly, in case of low storage
- [ ] pipeline for specifying filters
  - fields to keep
  - normalize fields between submissions and comments
  - subreddits, threads, users, contribution types to keep
  - organize output by subreddit, thread, user, contribution type

Resources
---------
* https://files.pushshift.io/reddit/comments/
* https://medium.com/@MaLiN2223/getting-data-from-pushshift-archives-b3bc0e487359
* https://github.com/facebookresearch/ELI5/blob/main/data_creation/download_reddit_qalist.py
* https://github.com/hide-ous/redditBots/blob/main/collect_ground_truth/collect_pushshift_users.py



About this project
-------

Free software: MIT license

Documentation: https://pullshift.readthedocs.io.

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _Pushshift: https://github.com/pushshift/api
.. _`archive files`: https://files.pushshift.io/reddit
