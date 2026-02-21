import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CowDialog } from './cow-dialog';

describe('CowDialog', () => {
  let component: CowDialog;
  let fixture: ComponentFixture<CowDialog>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CowDialog]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CowDialog);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
