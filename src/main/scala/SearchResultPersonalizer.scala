class SearchResultPersonalizer {

  def personalize(nonPersonalizedSearchResult: NonPersonalizedSearchResult): PersonalizedSearchResult = {
    PersonalizedSearchResult(nonPersonalizedSearchResult.items)
  }
}
