import { Vtctld2Page } from './app.po';

describe('vtctld2 App', function() {
  let page: Vtctld2Page;

  beforeEach(() => {
    page = new Vtctld2Page();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});
