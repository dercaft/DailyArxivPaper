import streamlit as st

# Streamlit app
def main():
    st.title("arXiv Paper Search")
    st.write("Search arXiv papers using either keyword-based search or semantic search.")

    # Sidebar for user inputs
    search_type = st.sidebar.selectbox("Search Type", ["Keyword Search", "Semantic Search"])
    user_query = st.sidebar.text_input("Enter your query", value="")

    if search_type == "Keyword Search" and user_query:
        st.subheader("Keyword Search Results")
        # Mock data for keyword search
        papers = [
            {
                "title": "Deep Learning for Image Recognition",
                "summary_ai": "This paper explores deep learning techniques for image recognition tasks.",
                "primary_category_code": "cs.CV",
                "arxiv_published_at": "2023-01-15",
                "pdf_url": "https://arxiv.org/pdf/1234.5678",
            },
            {
                "title": "Neural Networks in Natural Language Processing",
                "summary_ai": "An overview of neural network architectures for NLP applications.",
                "primary_category_code": "cs.CL",
                "arxiv_published_at": "2023-02-10",
                "pdf_url": "https://arxiv.org/pdf/2345.6789",
            },
        ]
        if papers:
            for paper in papers:
                st.write(f"**Title**: {paper['title']}")
                st.write(f"**Summary**: {paper['summary_ai']}")
                st.write(f"**Category**: {paper['primary_category_code']}")
                st.write(f"**Published At**: {paper['arxiv_published_at']}")
                st.write(f"[Read more]({paper['pdf_url']})")
                st.markdown("---")
        else:
            st.write("No results found.")

    elif search_type == "Semantic Search" and user_query:
        st.subheader("Semantic Search Results")
        # Mock data for semantic search
        results = [
            {
                "title": "Generative Adversarial Networks",
                "summary_ai": "This paper introduces GANs for generating realistic images.",
                "primary_category_code": "cs.LG",
                "arxiv_published_at": "2023-03-01",
                "pdf_url": "https://arxiv.org/pdf/3456.7890",
                "distance": 0.1234,
            },
            {
                "title": "Transformer Models for Machine Translation",
                "summary_ai": "A comprehensive study on Transformer models in MT.",
                "primary_category_code": "cs.CL",
                "arxiv_published_at": "2023-04-10",
                "pdf_url": "https://arxiv.org/pdf/4567.8901",
                "distance": 0.2345,
            },
        ]
        if results:
            for paper in results:
                st.write(f"**Title**: {paper['title']}")
                st.write(f"**Summary**: {paper['summary_ai']}")
                st.write(f"**Category**: {paper['primary_category_code']}")
                st.write(f"**Published At**: {paper['arxiv_published_at']}")
                st.write(f"**Score**: {paper['distance']:.4f}")
                st.write(f"[Read more]({paper['pdf_url']})")
                st.markdown("---")
        else:
            st.write("No results found.")

    else:
        st.write("Enter a query to start searching.")

if __name__ == "__main__":
    main()