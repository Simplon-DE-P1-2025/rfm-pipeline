import streamlit as st

st.set_page_config(
    page_title="Pipeline RFM",
    page_icon="🔁",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.success("Choisissez une page ci-dessus.")

st.title("🔁 Pipeline Data RFM")
st.markdown(
    """
    ### Bienvenue sur le dashboard de la pipeline RFM

    Utilisez la **barre latérale** pour naviguer entre les pages :

    | Page | Description |
    |------|-------------|
    | 🎓 Présentation | Support de présentation jury — architecture & concepts |
    | 📊 Dashboard RFM | Visualisation des scores et segments clients |
    | 🔍 Explorateur SQL | Requêtes libres sur les données brutes et transformées |
    """
)
