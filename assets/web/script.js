class SearchEngine {
  constructor() {
    this.searchForm = document.getElementById("searchForm");
    this.searchInput = document.getElementById("searchInput");
    this.searchButton = document.getElementById("searchButton");
    this.loading = document.getElementById("loading");
    this.errorMessage = document.getElementById("errorMessage");
    this.results = document.getElementById("results");
    this.resultsCount = document.getElementById("resultsCount");
    this.resultsList = document.getElementById("resultsList");

    this.init();
  }

  init() {
    this.searchForm.addEventListener("submit", (e) => {
      e.preventDefault();
      this.performSearch();
    });

    // Focus on search input on load
    this.searchInput.focus();
  }

  async performSearch() {
    const query = this.searchInput.value.trim();

    if (!query) {
      this.showError("Please enter a search query");
      return;
    }

    this.setLoading(true);
    this.hideError();
    this.hideResults();

    try {
      const response = await fetch("/query", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: query,
          limit: 20,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(
          errorData.error || `HTTP ${response.status}: ${response.statusText}`,
        );
      }

      const data = await response.json();
      this.displayResults(data.rankings, query);
    } catch (error) {
      console.error("Search error:", error);
      this.showError(`Search failed: ${error.message}`);
    } finally {
      this.setLoading(false);
    }
  }

  setLoading(isLoading) {
    this.loading.style.display = isLoading ? "block" : "none";
    this.searchButton.disabled = isLoading;
    this.searchInput.disabled = isLoading;

    if (isLoading) {
      this.searchButton.textContent = "Searching...";
    } else {
      this.searchButton.textContent = "Search";
    }
  }

  showError(message) {
    this.errorMessage.textContent = message;
    this.errorMessage.style.display = "block";
  }

  hideError() {
    this.errorMessage.style.display = "none";
  }

  displayResults(rankings, query) {
    if (!rankings || rankings.length === 0) {
      this.showNoResults(query);
      return;
    }

    const resultText = rankings.length === 1 ? "result" : "results";
    this.resultsCount.textContent = `Found ${rankings.length} ${resultText} for "${query}"`;

    this.resultsList.innerHTML = "";
    rankings.forEach((result, index) => {
      const resultElement = this.createResultElement(result, index + 1);
      this.resultsList.appendChild(resultElement);
    });

    this.showResults();
  }

  showNoResults(query) {
    this.resultsCount.textContent = `No results found for "${query}"`;
    this.resultsList.innerHTML =
      '<div class="no-results">Try different keywords or check your spelling</div>';
    this.showResults();
  }

  createResultElement(result, position) {
    const resultDiv = document.createElement("div");
    resultDiv.className = "result-item";

    const title = result.title || "Untitled";
    const snippet = result.snippet || "No description available";
    const score = result.score.toFixed(4);

    resultDiv.innerHTML = `
            <div class="result-title">
                <a href="${result.url}" target="_blank" rel="noopener noreferrer">
                    ${position}. ${this.escapeHtml(title)}
                </a>
            </div>
            <div class="result-url">${this.escapeHtml(result.url)}</div>
            <div class="result-snippet">${this.escapeHtml(snippet)}</div>
            <div class="result-meta">
                <span class="result-score">BM25 Score: ${score}</span>
                <span>Length: ${result.len} terms</span>
            </div>
        `;

    return resultDiv;
  }

  showResults() {
    this.results.style.display = "block";
  }

  hideResults() {
    this.results.style.display = "none";
  }

  escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
  }
}

// Initialize the search engine when the page loads
document.addEventListener("DOMContentLoaded", () => {
  new SearchEngine();
});
