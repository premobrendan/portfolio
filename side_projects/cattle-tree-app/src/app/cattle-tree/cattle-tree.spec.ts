import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CattleTree } from './cattle-tree';

describe('CattleTree', () => {
  let component: CattleTree;
  let fixture: ComponentFixture<CattleTree>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CattleTree]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CattleTree);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
