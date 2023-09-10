# graphics

## Description

This directory contains the code to generate the WEB application that displays the graphics with Streamlit under the hood.

## How to run

To run the application, you need to install the requirements in the `requirements.txt` file. You can do this by running the following command:

```bash
python3 -m pip install -e .
```

Then, you can run the test applications by running the following command:

```bash
alias st='streamlit run  --server.headless true --theme.base light '
# This start code below is referencing the file graph-01.py and graph-02.py
st graphics/graph-01.py
st graphics/graph-02.py
# You can use Raw URI from GitHub repository like this:
# st https://raw.githubusercontent.com//joao-parana/...whatever-file.py
```

And open the URI in browser.

**Streamlit** implements a series of features to simplify the development of
data analysis applications, including layout management with containers such
as sidebars and tabs, as well as a set of very useful widgets.

With each user interaction with a given widget, the Framework reevaluates the
page and therefore it is necessary to use `@st.cache_data` and `st.cache_resource`
to avoid costly data and I/O reprocessing.

Caching an state manegement are some of the most important features of Streamlit.

For information on **caching** see [https://docs.streamlit.io/library/advanced-features/caching](https://docs.streamlit.io/library/advanced-features/caching)

